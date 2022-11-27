use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{PiperError, Value, ValueType};

/**
 * The column definition
 */
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Column {
    /**
     * Column name
     */
    pub name: String,

    /**
     * Column type
     */
    pub column_type: ValueType,
}

impl Column {
    pub fn new<T>(name: T, column_type: ValueType) -> Self
    where
        T: ToString,
    {
        Self {
            name: name.to_string(),
            column_type,
        }
    }
}

/**
 * Schema is a collection of columns
 */
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl<T> From<T> for Schema
where
    T: IntoIterator<Item = Column>,
{
    fn from(columns: T) -> Self {
        Self {
            columns: columns.into_iter().collect(),
        }
    }
}

impl FromIterator<Column> for Schema {
    fn from_iter<T: IntoIterator<Item = Column>>(iter: T) -> Self {
        Self {
            columns: iter.into_iter().collect(),
        }
    }
}

impl Schema {
    pub fn get_column_types(&self) -> Vec<ValueType> {
        self.columns.iter().map(|c| c.column_type).collect()
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|column| column.name == column_name)
    }

    pub fn dump(&self) -> String {
        self.columns
            .iter()
            .map(|c| format!("{} as {}", c.name, c.column_type))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/**
 * The DataSet interface
 * A DataSet is a collection of rows, each row is a collection of fields.
 * DataSet works like an iterator, it can only be used once.
 */
#[async_trait]
pub trait DataSet: Sync + Send {
    /**
     * Get the schema of the data set
     */
    fn schema(&self) -> &Schema;

    /**
     * Get the next row of the data set, returns None if there is no more row
     */
    async fn next(&mut self) -> Option<Vec<Value>>;

    /**
     * Get all rows of the data set
     */
    async fn eval(&mut self) -> (Schema, Vec<Vec<Value>>) {
        let mut rows = Vec::new();
        while let Some(row) = self.next().await {
            rows.push(row);
        }
        (self.schema().clone(), rows)
    }

    async fn dump(&mut self) -> String {
        let mut ret = String::new();
        let s = self.schema().dump();
        ret.push_str(&s);
        ret.push('\n');
        ret.push_str("-".repeat(s.len()).as_str());
        ret.push('\n');
        self.next().await.into_iter().for_each(|row| {
            ret.push_str(
                row.iter()
                    .map(|v| v.dump())
                    .collect::<Vec<_>>()
                    .join(", ")
                    .as_str(),
            );
            ret.push('\n');
        });
        ret
    }
}

/**
 * Validate if the data set is aligned with the schema
 */
#[derive(Copy, Debug, Clone)]
pub enum ValidationMode {
    /**
     * Strict mode turns every field that doesn't match the schema into error
     */
    Strict,
    /**
     * Lenient mode tries to convert the field into the schema type
     */
    Lenient,
}

pub struct ValidatedDataSet {
    data_set: Box<dyn DataSet>,
    mode: ValidationMode,
}

impl ValidatedDataSet {
    pub fn new(data_set: Box<dyn DataSet>, mode: ValidationMode) -> Self {
        Self { data_set, mode }
    }
}

pub trait Validated {
    fn validated(self, mode: ValidationMode) -> Box<dyn DataSet>;
}

impl Validated for Box<dyn DataSet> {
    fn validated(self, mode: ValidationMode) -> Box<dyn DataSet> {
        Box::new(ValidatedDataSet::new(self, mode))
    }
}

#[async_trait]
impl DataSet for ValidatedDataSet {
    fn schema(&self) -> &Schema {
        self.data_set.schema()
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        self.data_set.next().await.map(|mut row| {
            // Make sure row is not longer than schema
            row.truncate(self.schema().columns.len());
            // Some fields may be missing
            let missing = row.len()..self.schema().columns.len();
            row.into_iter()
                .enumerate()
                .map(|(idx, v)| {
                    let column_type = self.schema().columns[idx].column_type;
                    if column_type == ValueType::Dynamic || column_type == v.value_type() {
                        v
                    } else {
                        match self.mode {
                            ValidationMode::Strict => v.cast_to(column_type),
                            ValidationMode::Lenient => v.convert_to(column_type),
                        }
                    }
                })
                .chain(missing.map(|idx| {
                    // Fill missing fields with error
                    Value::Error(PiperError::ValidationError(format!(
                        "Column {} is missing in the input data set",
                        self.schema().columns[idx].name,
                    )))
                }))
                .collect()
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorCollectingMode {
    Off,
    // TODO: Remove this alias when the debug mode is actually implemented
    #[serde(alias = "debug")]
    On,
    // TODO: Real debug mode needs backtrace
    // Debug,
}

impl Default for ErrorCollectingMode {
    fn default() -> Self {
        ErrorCollectingMode::On
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorRecord {
    pub row: usize,
    pub column: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

pub trait ErrorCollector {
    fn collect_errors(
        self,
        mode: ErrorCollectingMode,
    ) -> (Schema, Vec<Vec<Value>>, Vec<ErrorRecord>);

    fn collect_into_json(
        self,
        mode: ErrorCollectingMode,
    ) -> (Vec<HashMap<String, serde_json::Value>>, Vec<ErrorRecord>);
}

impl ErrorCollector for (Schema, Vec<Vec<Value>>) {
    fn collect_errors(
        self,
        mode: ErrorCollectingMode,
    ) -> (Schema, Vec<Vec<Value>>, Vec<ErrorRecord>) {
        if mode == ErrorCollectingMode::Off {
            return (self.0, self.1, vec![]);
        }
        let mut errors = Vec::new();
        for (row_num, row) in self.1.iter().enumerate() {
            for (i, value) in row.iter().enumerate() {
                if let Value::Error(err) = value {
                    errors.push(ErrorRecord {
                        row: row_num,
                        column: self.0.columns[i].name.clone(),
                        message: err.to_string(),
                        details: None, // TODO: Debug mode, save backtrace info here
                    });
                }
            }
        }
        (self.0, self.1, errors)
    }

    fn collect_into_json(
        self,
        mode: ErrorCollectingMode,
    ) -> (Vec<HashMap<String, serde_json::Value>>, Vec<ErrorRecord>) {
        let mut ret = vec![];
        let mut errors = Vec::new();
        for (row_num, row) in self.1.into_iter().enumerate() {
            let mut ret_row = HashMap::new();
            for (i, value) in row.into_iter().enumerate() {
                if let Value::Error(err) = value {
                    ret_row.insert(self.0.columns[i].name.clone(), serde_json::Value::Null);
                    if mode != ErrorCollectingMode::Off {
                        errors.push(ErrorRecord {
                            row: row_num,
                            column: self.0.columns[i].name.clone(),
                            message: err.to_string(),
                            details: None, // TODO: Debug mode, save backtrace info here
                        });
                    }
                } else {
                    ret_row.insert(self.0.columns[i].name.clone(), value.into());
                }
            }
            ret.push(ret_row);
        }
        (ret, errors)
    }
}

/**
 * Some common operations to create a data set
 */
pub struct DataSetCreator;

#[allow(dead_code)]
impl DataSetCreator {
    /**
     * Create an empty data set which contains no row
     */
    pub fn empty(schema: Schema) -> Box<dyn DataSet> {
        EagerDataSet::new(schema, vec![])
    }

    /**
     * Create a data set from a vector of rows
     */
    pub fn eager<T>(schema: Schema, rows: T) -> Box<dyn DataSet>
    where
        T: IntoIterator<Item = Vec<Value>>,
    {
        EagerDataSet::new(schema, rows.into_iter().collect())
    }

    /**
     * Create a data set from an iterator
     */
    pub fn from<T>(rows: T) -> Box<dyn DataSet>
    where
        T: IntoIterator<Item = Vec<Value>>,
    {
        rows.into_iter().collect::<Box<EagerDataSet>>()
    }
}

#[derive(Clone, Debug)]
struct EagerDataSet {
    schema: Schema,
    rows: VecDeque<Vec<Value>>,
}

impl EagerDataSet {
    fn new(schema: Schema, rows: Vec<Vec<Value>>) -> Box<Self> {
        Box::new(Self {
            schema,
            rows: rows.into(),
        })
    }
}

impl FromIterator<Vec<Value>> for Box<EagerDataSet> {
    fn from_iter<T: IntoIterator<Item = Vec<Value>>>(iter: T) -> Self {
        let mut rows = vec![];
        let mut schema = None;
        let mut col = 0;
        for row in iter {
            if schema.is_none() {
                schema = Some(Schema::from_iter(row.iter().map(|v| {
                    col += 1;
                    Column::new(format!("col{}", col), v.value_type())
                })));
            }
            rows.push(row);
        }
        Box::new(EagerDataSet {
            schema: schema.unwrap_or_default(), // In case nothing is provided
            rows: rows.into(),
        })
    }
}

impl From<Vec<Vec<Value>>> for Box<EagerDataSet> {
    fn from(rows: Vec<Vec<Value>>) -> Self {
        let schema: Schema = rows[0]
            .iter()
            .enumerate()
            .map(|(i, v)| Column::new(i, v.value_type()))
            .collect();
        EagerDataSet::new(schema, rows)
    }
}

#[async_trait]
impl DataSet for EagerDataSet {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        self.rows.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gen_schema() -> Schema {
        vec![
            Column::new("col1", ValueType::Int),
            Column::new("col2", ValueType::String),
            Column::new("col2", ValueType::Bool),
        ]
        .into()
    }

    fn gen_ds() -> Box<dyn DataSet> {
        DataSetCreator::eager(
            gen_schema(),
            vec![
                vec![Value::from(10), Value::from(100), Value::from(true)],
                vec![Value::from(20), Value::from("foo"), Value::from(true)],
                vec![Value::from(30), Value::from(300), Value::from(false)],
                vec![Value::from(40), Value::from(400), Value::from(false)],
                vec![Value::from(50), Value::from(500), Value::from(false)],
                vec![Value::from(60), Value::from("bar"), Value::from(false)],
                vec![Value::from(70), Value::from(700), Value::from(true)],
            ],
        )
    }

    #[tokio::test]
    async fn test_strict_validate() {
        let (schema, rows) = gen_ds().validated(ValidationMode::Strict).eval().await;
        assert_eq!(schema, gen_schema());
        assert_eq!(rows.len(), 7);
        assert!(matches!(
            rows[0].as_slice(),
            [Value::Int(10), Value::Error(_), Value::Bool(true)]
        ));
        assert!(matches!(
            rows[1].as_slice(),
            [Value::Int(20), Value::String(_), Value::Bool(true)]
        ));
        assert!(matches!(
            rows[2].as_slice(),
            [Value::Int(30), Value::Error(_), Value::Bool(false)]
        ));
        assert!(matches!(
            rows[3].as_slice(),
            [Value::Int(40), Value::Error(_), Value::Bool(false)]
        ));
        assert!(matches!(
            rows[4].as_slice(),
            [Value::Int(50), Value::Error(_), Value::Bool(false)]
        ));
        assert!(matches!(
            rows[5].as_slice(),
            [Value::Int(60), Value::String(_), Value::Bool(false)]
        ));
        assert!(matches!(
            rows[6].as_slice(),
            [Value::Int(70), Value::Error(_), Value::Bool(true)]
        ));
    }

    #[tokio::test]
    async fn test_lenient_validate() {
        let (schema, rows) = gen_ds().validated(ValidationMode::Lenient).eval().await;
        assert_eq!(schema, gen_schema());
        assert_eq!(rows.len(), 7);
        assert!(matches!(
            rows[0].as_slice(),
            [Value::Int(10), Value::String(_), Value::Bool(true)]
        ));
        assert!(matches!(
            rows[1].as_slice(),
            [Value::Int(20), Value::String(_), Value::Bool(true)]
        ));
        assert!(matches!(
            rows[2].as_slice(),
            [Value::Int(30), Value::String(_), Value::Bool(false)]
        ));
        assert!(matches!(
            rows[3].as_slice(),
            [Value::Int(40), Value::String(_), Value::Bool(false)]
        ));
        assert!(matches!(
            rows[4].as_slice(),
            [Value::Int(50), Value::String(_), Value::Bool(false)]
        ));
        assert!(matches!(
            rows[5].as_slice(),
            [Value::Int(60), Value::String(_), Value::Bool(false)]
        ));
        assert!(matches!(
            rows[6].as_slice(),
            [Value::Int(70), Value::String(_), Value::Bool(true)]
        ));
    }
}
