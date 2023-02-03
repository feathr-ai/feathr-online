use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{pin_mut, Future};
use piper::{Logged, RequestData};
use pyo3::exceptions::PyException;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{create_exception, prelude::*};
use serde_json::json;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

create_exception!(piper, PiperError, PyException);

/**
 * Check CTRL-C every 100ms, cancel the future if pressed and return Interrupted error
 */
async fn cancelable_wait<F, T: Send>(f: F) -> PyResult<T>
where
    F: Future<Output = PyResult<T>>,
{
    // Future needs to be pinned then its mutable ref can be awaited multiple times.
    pin_mut!(f);
    loop {
        match tokio::time::timeout(std::time::Duration::from_millis(100), &mut f).await {
            Ok(v) => {
                return v;
            }
            Err(_) => {
                // Timeout, check if CTRL-C is pressed
                Python::with_gil(|py| py.check_signals())?
            }
        }
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    match Handle::try_current() {
        Ok(handle) => handle.block_on(future),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future),
    }
}

#[derive(Debug)]
struct PyLookupSource {
    lookup_fun: PyObject,
}

impl PyLookupSource {
    fn new(lookup_fun: &PyAny, py: Python<'_>) -> PyResult<Self> {
        let ret = py
            .import("asyncio")?
            .call_method1("iscoroutinefunction", (lookup_fun,))?
            .extract::<bool>()?;
        if !ret {
            return Err(PiperError::new_err(
                "lookup_fun must be an async coroutine function",
            ));
        }
        Ok(Self {
            lookup_fun: lookup_fun.into_py(py),
        })
    }
}

#[async_trait]
impl piper::LookupSource for PyLookupSource {
    fn dump(&self) -> serde_json::Value {
        json!(
            {
                "type": "python",
                "lookup_fun": self.lookup_fun.to_string(),
            }
        )
    }

    async fn join(&self, key: &piper::Value, fields: &[String]) -> Vec<Vec<piper::Value>> {
        let fields = fields.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let fut = Python::with_gil(|py| {
            self.lookup_fun
                .call(
                    py,
                    (
                        Value(key.to_owned()).into_py(py),
                        fields.clone().into_py(py),
                    ),
                    None,
                )
                .and_then(|c| pyo3_asyncio::tokio::into_future(c.into_ref(py)))
        });
        let r = match fut {
            Ok(fut) => fut.await,
            Err(e) => Err(e),
        };
        match r {
            Ok(v) => Python::with_gil(|py| {
                let v = match v.extract::<Py<PyList>>(py) {
                    Ok(v) => v,
                    Err(e) => {
                        return vec![vec![
                            piper::Value::Error(piper::PiperError::ExternalError(
                                e.to_string()
                            ));
                            fields.len()
                        ]];
                    }
                };
                let mut rows = vec![];
                for src_row in v.as_ref(py).iter() {
                    let mut row = vec![];
                    match src_row.extract::<Py<PyList>>() {
                        Ok(r) => {
                            for e in r.as_ref(py).iter() {
                                let e = match e.extract::<Value>() {
                                    Ok(v) => v,
                                    Err(e) => Value(piper::Value::Error(
                                        piper::PiperError::ExternalError(e.to_string()),
                                    )),
                                };
                                row.push(e.0);
                            }
                        }
                        Err(e) => {
                            row = vec![
                                piper::Value::Error(piper::PiperError::ExternalError(
                                    e.to_string()
                                ));
                                fields.len()
                            ];
                        }
                    }
                    rows.push(row);
                }
                rows
            }),
            Err(e) => vec![vec![
                piper::Value::Error(piper::PiperError::ExternalError(
                    e.to_string()
                ));
                fields.len()
            ]],
        }
    }

    async fn lookup(&self, key: &piper::Value, fields: &[String]) -> Vec<piper::Value> {
        self.join(key, fields)
            .await
            .into_iter()
            .next()
            .unwrap_or_else(|| vec![piper::Value::Null; fields.len()])
    }
}

fn dict_to_lookup_source(d: &PyDict, py: Python<'_>) -> PyResult<Arc<dyn piper::LookupSource>> {
    let js = py.import("json")?.call_method1("dumps", (d,))?;
    let js = js.into_py(py).extract::<String>(py)?;
    piper::load_lookup_source(&js).map_err(|e| PiperError::new_err(e.to_string()))
}

fn obj_to_lookup_source(o: &PyObject, py: Python<'_>) -> PyResult<Arc<dyn piper::LookupSource>> {
    match o.extract::<String>(py) {
        Ok(s) => piper::load_lookup_source(&s).map_err(|e| PiperError::new_err(e.to_string())),
        Err(_) => match o.into_py(py).extract::<Py<PyDict>>(py) {
            Ok(d) => dict_to_lookup_source(d.as_ref(py).extract::<Py<PyDict>>()?.as_ref(py), py),
            Err(_) => Ok(Arc::new(PyLookupSource::new(o.as_ref(py), py)?)),
        },
    }
}

#[pyclass(module = "feathrpiper")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ErrorCollectingMode {
    On,
    Off,
}

impl Default for ErrorCollectingMode {
    fn default() -> Self {
        Self::On
    }
}

impl From<piper::ErrorCollectingMode> for ErrorCollectingMode {
    fn from(mode: piper::ErrorCollectingMode) -> Self {
        match mode {
            piper::ErrorCollectingMode::On => ErrorCollectingMode::On,
            piper::ErrorCollectingMode::Off => ErrorCollectingMode::Off,
        }
    }
}

impl From<ErrorCollectingMode> for piper::ErrorCollectingMode {
    fn from(mode: ErrorCollectingMode) -> Self {
        match mode {
            ErrorCollectingMode::On => piper::ErrorCollectingMode::On,
            ErrorCollectingMode::Off => piper::ErrorCollectingMode::Off,
        }
    }
}

#[repr(transparent)]
struct Value(piper::Value);

impl<'source> FromPyObject<'source> for Value {
    fn extract(ob: &PyAny) -> PyResult<Self> {
        if ob.is_none() {
            return Ok(Value(piper::Value::Null));
        }
        if let Ok(v) = ob.extract::<bool>() {
            Ok(Value(piper::Value::Bool(v)))
        } else if let Ok(v) = ob.extract::<i64>() {
            Ok(Value(piper::Value::Long(v)))
        } else if let Ok(v) = ob.extract::<f64>() {
            Ok(Value(piper::Value::Double(v)))
        } else if let Ok(v) = ob.extract::<String>() {
            Ok(Value(piper::Value::from(v)))
        } else if let Ok(v) = ob.extract::<DateTime<Utc>>() {
            Ok(Value(piper::Value::DateTime(v)))
        } else if let Ok(v) = ob.extract::<Vec<Value>>() {
            Ok(Value(v.into_iter().map(|v| v.0).collect()))
        } else if let Ok(v) = ob.extract::<HashMap<String, Value>>() {
            let m = piper::Value::Object(v.into_iter().map(|(k, v)| (k, v.0)).collect());
            Ok(Value(m))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Unsupported type",
            ))
        }
    }
}

impl IntoPy<PyObject> for Value {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            piper::Value::Null => py.None(),
            piper::Value::Bool(v) => v.into_py(py),
            piper::Value::Int(v) => v.into_py(py),
            piper::Value::Long(v) => v.into_py(py),
            piper::Value::Float(v) => v.into_py(py),
            piper::Value::Double(v) => v.into_py(py),
            piper::Value::String(v) => v.into_py(py),
            piper::Value::DateTime(v) => v.into_py(py),
            piper::Value::Array(v) => v.into_iter().map(Value).collect::<Vec<_>>().into_py(py),
            piper::Value::Object(v) => v
                .into_iter()
                .map(|(k, v)| (k, Value(v)))
                .collect::<HashMap<_, _>>()
                .into_py(py),
            piper::Value::Error(v) => PyErr::new::<PiperError, _>(v.to_string()).to_object(py),
        }
    }
}

#[derive(Clone)]
struct PyPiperFunction {
    function: PyObject,
}

impl piper::Function for PyPiperFunction {
    fn get_output_type(
        &self,
        _argument_types: &[piper::ValueType],
    ) -> Result<piper::ValueType, piper::PiperError> {
        Ok(piper::ValueType::Dynamic)
    }

    fn eval(&self, arguments: Vec<piper::Value>) -> piper::Value {
        Python::with_gil(|py| {
            let args = PyTuple::new(py, arguments.into_iter().map(|v| Value(v).into_py(py)));
            self.function
                .call1(py, args)
                .map(|v| {
                    v.extract::<Value>(py).map(|v| v.0).unwrap_or_else(|e| {
                        piper::Value::Error(piper::PiperError::ExternalError(e.to_string()))
                    })
                })
                .unwrap_or_else(|e| {
                    piper::Value::Error(piper::PiperError::ExternalError(e.to_string()))
                })
        })
    }
}

fn dict_to_request(
    pipeline: &str,
    dict: &PyDict,
    error_report: ErrorCollectingMode,
) -> PyResult<piper::SingleRequest> {
    let mut data: HashMap<String, serde_json::Value> = HashMap::new();
    for (k, v) in dict {
        let k = k.extract::<String>()?;
        let v = v.extract::<Value>()?.0;
        data.insert(k, v.into());
    }
    Ok(piper::SingleRequest {
        pipeline: pipeline.to_string(),
        errors: error_report.into(),
        data: RequestData::Single(data),
        ..Default::default()
    })
}

fn list_to_request(
    py: Python<'_>,
    pipeline: &str,
    list: &PyList,
    error_report: ErrorCollectingMode,
) -> PyResult<piper::SingleRequest> {
    let mut data: Vec<HashMap<String, serde_json::Value>> = Vec::new();
    for dict in list {
        let mut row: HashMap<String, serde_json::Value> = HashMap::new();
        for (k, v) in dict.extract::<Py<PyDict>>()?.as_ref(py) {
            let k = k.extract::<String>()?;
            let v = v.extract::<Value>()?.0;
            row.insert(k, v.into());
        }
        data.push(row);
    }
    Ok(piper::SingleRequest {
        pipeline: pipeline.to_string(),
        errors: error_report.into(),
        data: RequestData::Multi(data),
        ..Default::default()
    })
}

fn pyobj_to_request(
    py: Python<'_>,
    pipeline: &str,
    obj: PyObject,
    error_report: ErrorCollectingMode,
) -> PyResult<piper::SingleRequest> {
    if let Ok(dict) = obj.extract::<Py<PyDict>>(py) {
        dict_to_request(pipeline, dict.as_ref(py), error_report)
    } else if let Ok(list) = obj.extract::<Py<PyList>>(py) {
        list_to_request(py, pipeline, list.as_ref(py), error_report)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Must be a dict or a list of dicts",
        ))
    }
}

fn error_record_to_dict(py: Python<'_>, e: piper::ErrorRecord) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("row", e.row)?;
    dict.set_item("column", e.column)?;
    dict.set_item("message", e.message)?;
    Ok(dict.into())
}

fn response_to_tuple(py: Python<'_>, response: piper::SingleResponse) -> PyResult<Py<PyTuple>> {
    let errors = PyList::empty(py);
    for e in response.errors {
        errors.append(error_record_to_dict(py, e)?)?;
    }
    let list = PyList::empty(py);
    for row in response.data.unwrap_or_default() {
        let dict = PyDict::new(py);
        for (k, v) in row {
            dict.set_item(k, Value(piper::Value::from(v)).into_py(py))?;
        }
        list.append(dict)?;
    }
    let t = PyTuple::new(py, [list, errors]);
    Ok(t.into())
}

#[repr(transparent)]
struct SingleResponse(piper::SingleResponse);

impl IntoPy<PyObject> for SingleResponse {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match response_to_tuple(py, self.0) {
            Ok(t) => t.to_object(py),
            Err(e) => e.to_object(py),
        }
    }
}

#[pyclass(module = "feathrpiper")]
struct Piper {
    pipelines: String,
    lookups: PyObject,
    functions: HashMap<String, PyObject>,
    piper: Option<Arc<piper::Piper>>,
}

#[pymethods]
impl Piper {
    #[new]
    #[args(pipelines = "None", lookups = "None", functions = "HashMap::new()")]
    fn new(
        pipelines: Option<&str>,
        lookups: Option<PyObject>,
        functions: HashMap<String, PyObject>,
        py: Python<'_>,
    ) -> PyResult<Self> {
        if pipelines.is_none() {
            return Ok(Self {
                pipelines: "".to_string(),
                lookups: py.None(),
                functions: HashMap::new(),
                piper: None,
            });
        }
        let lookups = match lookups {
            Some(lookups) => lookups,
            None => PyDict::new(py).into_py(py),
        };
        let l = lookups.clone();
        let f = functions.clone();
        let functions = functions
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    Box::new(PyPiperFunction { function: v }) as Box<dyn piper::Function>,
                )
            })
            .collect();
        match lookups.as_ref(py).extract::<String>() {
            Ok(lookups) => Ok(Self {
                pipelines: pipelines.as_ref().unwrap().to_string(),
                lookups: l,
                functions: f,
                piper: Some(Arc::new(
                    piper::Piper::new_with_udf(pipelines.unwrap(), &lookups, functions)
                        .map_err(|e| PyErr::new::<PiperError, _>(e.to_string()))?,
                )),
            }),
            Err(_) => {
                let lookups = lookups.as_ref(py).extract::<Py<PyDict>>()?;
                let lookups = lookups
                    .as_ref(py)
                    .into_iter()
                    .map(|(k, v)| {
                        let k = k.extract::<String>()?;
                        let v = obj_to_lookup_source(&v.into_py(py), py);
                        v.map(|v| (k, v))
                    })
                    .collect::<PyResult<HashMap<_, _>>>()?;
                Ok(Self {
                    pipelines: pipelines.as_ref().unwrap().to_string(),
                    lookups: l,
                    functions: f,
                    piper: Some(Arc::new(
                        piper::Piper::new_with_lookup_udf(pipelines.unwrap(), lookups, functions)
                            .map_err(|e| PyErr::new::<PiperError, _>(e.to_string()))?,
                    )),
                })
            }
        }
    }

    fn __getstate__(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("pipelines", self.pipelines.clone()).log()?;
        dict.set_item("lookups", self.lookups.clone()).log()?;
        dict.set_item("functions", self.functions.clone()).log()?;
        Ok(dict.into())
    }

    fn __setstate__(&mut self, state: PyObject, py: Python<'_>) -> PyResult<()> {
        let state = state.extract::<Py<PyDict>>(py)?;
        let pipelines: String = state
            .as_ref(py)
            .get_item("pipelines")
            .ok_or_else(|| PyErr::new::<PiperError, _>("Missing field 'pipelines'"))
            .and_then(|v| v.extract())?;
        let lookups: PyObject = state
            .as_ref(py)
            .get_item("lookups")
            .ok_or_else(|| PyErr::new::<PiperError, _>("Missing field 'lookups'"))
            .and_then(|v| v.extract())?;
        let functions: HashMap<String, PyObject> = state
            .as_ref(py)
            .get_item("functions")
            .ok_or_else(|| PyErr::new::<PiperError, _>("Missing field 'functions'"))
            .and_then(|v| v.extract())?;
        let new_me = Self::new(
            Some(&pipelines),
            Some(lookups.clone()),
            functions.clone(),
            py,
        )?;
        self.pipelines = pipelines;
        self.lookups = lookups;
        self.functions = functions;
        self.piper = new_me.piper;
        Ok(())
    }

    #[args(error_report = "ErrorCollectingMode::default()")]
    fn process(
        &self,
        pipeline: &str,
        object: PyObject,
        error_report: ErrorCollectingMode,
        py: Python<'_>,
    ) -> PyResult<Py<PyTuple>> {
        if self.piper.is_none() {
            return Err(PyErr::new::<PiperError, _>(
                "Piper has not been initialized",
            ));
        }
        let req = pyobj_to_request(py, pipeline, object, error_report)?;
        let resp = py.allow_threads(|| {
            block_on(cancelable_wait(async move {
                self.piper
                    .as_ref()
                    .unwrap()
                    .process_single_request(req)
                    .await
                    .map_err(|e| PyErr::new::<PiperError, _>(e.to_string()))
            }))
        })?;
        response_to_tuple(py, resp)
    }

    #[args(error_report = "ErrorCollectingMode::default()")]
    fn process_async<'p>(
        &self,
        pipeline: &str,
        object: PyObject,
        error_report: ErrorCollectingMode,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        if self.piper.is_none() {
            return Err(PyErr::new::<PiperError, _>(
                "Piper has not been initialized",
            ));
        }
        let req = pyobj_to_request(py, pipeline, object, error_report)?;
        let piper = self.piper.clone();
        pyo3_asyncio::tokio::future_into_py(
            py,
            cancelable_wait(async move {
                piper
                    .unwrap()
                    .process_single_request(req)
                    .await
                    .map(SingleResponse)
                    .map_err(|e| PyErr::new::<PiperError, _>(e.to_string()))
            }),
        )
    }

    fn lookup(
        &self,
        source: &str,
        keys: Vec<Value>,
        fields: Vec<String>,
        py: Python<'_>,
    ) -> PyResult<Vec<HashMap<String, Value>>> {
        if self.piper.is_none() {
            return Err(PyErr::new::<PiperError, _>(
                "Piper has not been initialized",
            ));
        }
        let req = piper::LookupRequest {
            source: source.to_string(),
            keys: keys.into_iter().map(|v| v.0.into()).collect(),
            features: fields,
        };
        let resp = py.allow_threads(|| {
            block_on(cancelable_wait(async move {
                self.piper
                    .as_ref()
                    .unwrap()
                    .lookup(req)
                    .await
                    .map_err(|e| PyErr::new::<PiperError, _>(e.to_string()))
            }))
        })?;
        let data = resp
            .data
            .into_iter()
            .map(|v| v.into_iter().map(|(k, v)| (k, Value(v.into()))).collect())
            .collect();
        Ok(data)
    }

    fn lookup_async<'p>(
        &self,
        source: &str,
        keys: Vec<Value>,
        fields: Vec<String>,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        if self.piper.is_none() {
            return Err(PyErr::new::<PiperError, _>(
                "Piper has not been initialized",
            ));
        }
        let piper = self.piper.clone();
        let req = piper::LookupRequest {
            source: source.to_string(),
            keys: keys.into_iter().map(|v| v.0.into()).collect(),
            features: fields,
        };
        pyo3_asyncio::tokio::future_into_py(
            py,
            cancelable_wait(async move {
                let resp = piper
                    .unwrap()
                    .lookup(req)
                    .await
                    .map_err(|e| PyErr::new::<PiperError, _>(e.to_string()))?;
                let data: Vec<HashMap<String, Value>> = resp
                    .data
                    .into_iter()
                    .map(|v| v.into_iter().map(|(k, v)| (k, Value(v.into()))).collect())
                    .collect();
                Ok(data)
            }),
        )
    }

    #[getter]
    fn get_pipelines(&self, py: Python<'_>) -> PyResult<PyObject> {
        if self.piper.is_none() {
            return Err(PyErr::new::<PiperError, _>(
                "Piper has not been initialized",
            ));
        }

        let ret = PyDict::new(py);
        for (name, pipeline) in self.piper.as_ref().unwrap().pipelines.iter() {
            if name.starts_with('%') {
                continue;
            }
            let dict = PyDict::new(py);
            let input_schema = PyList::empty(py);
            for c in pipeline.input_schema.columns.iter() {
                let column = PyDict::new(py);
                column.set_item("name", c.name.clone()).unwrap();
                column.set_item("type", c.column_type.to_string()).unwrap();
                input_schema.append(column).unwrap();
            }
            dict.set_item("input_schema", input_schema).unwrap();
            let output_schema = PyList::empty(py);
            for c in pipeline.output_schema.columns.iter() {
                let column = PyDict::new(py);
                column.set_item("name", c.name.clone()).unwrap();
                column.set_item("type", c.column_type.to_string()).unwrap();
                output_schema.append(column).unwrap();
            }
            dict.set_item("output_schema", output_schema).unwrap();
            ret.set_item(name, dict).unwrap();
        }
        Ok(ret.into())
    }
}

#[pyclass(module = "feathrpiper")]
#[pyo3(text_signature = "(pipelines lookups functions /)")]
struct PiperService {
    service: Arc<RwLock<piper::PiperService>>,
}

#[pymethods]
impl PiperService {
    #[new]
    #[args(functions = "HashMap::new()")]
    fn new(
        pipelines: &str,
        lookups: PyObject,
        functions: HashMap<String, PyObject>,
        py: Python<'_>,
    ) -> PyResult<Self> {
        let functions = functions
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    Box::new(PyPiperFunction { function: v }) as Box<dyn piper::Function>,
                )
            })
            .collect();

        match lookups.as_ref(py).extract::<String>() {
            Ok(lookups) => Ok(Self {
                service: Arc::new(RwLock::new(piper::PiperService::create(
                    pipelines, &lookups, functions,
                ))),
            }),
            Err(_) => {
                let lookups = lookups.into_py(py).extract::<Py<PyDict>>(py)?;
                let lookups = lookups
                    .as_ref(py)
                    .into_iter()
                    .map(|(k, v)| {
                        let k = k.extract::<String>()?;
                        let v = obj_to_lookup_source(&v.into_py(py), py);
                        v.map(|v| (k, v))
                    })
                    .collect::<PyResult<HashMap<_, _>>>()?;
                Ok(Self {
                    service: Arc::new(RwLock::new(piper::PiperService::create_with_lookup_udf(
                        pipelines, lookups, functions,
                    ))),
                })
            }
        }
    }

    #[pyo3(text_signature = "($self address port /)")]
    fn start(&mut self, address: &str, port: u16, py: Python<'_>) -> PyResult<()> {
        let svc = self.service.clone();
        py.allow_threads(|| {
            block_on(cancelable_wait(async move {
                svc.write()
                    .await
                    .start_at(address, port, false)
                    .await
                    .map_err(|e| PyErr::new::<PiperError, _>(e.to_string()))
            }))
        })
    }

    #[pyo3(text_signature = "($self address port /)")]
    fn start_async<'p>(&mut self, address: &str, port: u16, py: Python<'p>) -> PyResult<&'p PyAny> {
        let svc = self.service.clone();
        let address = address.to_string();
        pyo3_asyncio::tokio::future_into_py(
            py,
            cancelable_wait(async move {
                svc.write()
                    .await
                    .start_at(&address, port, true)
                    .await
                    .map_err(|e| PyErr::new::<PiperError, _>(e.to_string()))
            }),
        )
    }

    #[pyo3(text_signature = "($self /)")]
    fn stop(&mut self) -> PyResult<()> {
        block_on(cancelable_wait(async move {
            self.service.write().await.stop();
            Ok(())
        }))
    }
}

#[pymodule]
#[pyo3(name = "feathrpiper")]
fn python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ErrorCollectingMode>()?;
    m.add_class::<Piper>()?;
    m.add_class::<PiperService>()?;
    Ok(())
}
