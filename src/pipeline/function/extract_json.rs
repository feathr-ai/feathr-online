use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Clone, Debug)]
pub struct ExtractJsonObject;

impl Function for ExtractJsonObject {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() > 2 || argument_types.is_empty() {
            return Err(PiperError::InvalidArgumentCount(2, argument_types.len()));
        }
        Ok(ValueType::Dynamic)
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        let s = match arguments[0].get_string() {
            Ok(v) => v,
            Err(e) => return e.into(),
        };
        let json: serde_json::Value = match serde_json::from_str(&s)
            .map_err(|e| PiperError::InvalidJsonString(e.to_string()))
        {
            Ok(v) => v,
            Err(e) => return e.into(),
        };
        let path = match arguments[1].get_string() {
            Ok(v) => v,
            Err(e) => return e.into(),
        };
        let ret = match jsonpath_lib::select(&json, &path)
            .map_err(|e| PiperError::InvalidJsonString(e.to_string()))
        {
            Ok(v) => v,
            Err(e) => return e.into(),
        };
        if ret.is_empty() {
            return Value::Null;
        }
        ret[0].clone().into()
    }
}

#[derive(Clone, Debug)]
pub struct ExtractJsonArray;

impl Function for ExtractJsonArray {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() > 2 || argument_types.is_empty() {
            return Err(PiperError::InvalidArgumentCount(2, argument_types.len()));
        }
        Ok(ValueType::Dynamic)
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        let s = match arguments[0].get_string() {
            Ok(v) => v,
            Err(e) => return e.into(),
        };
        let json: serde_json::Value = match serde_json::from_str(&s)
            .map_err(|e| PiperError::InvalidJsonString(format!("Invalid JSON: {}", e)))
        {
            Ok(v) => v,
            Err(e) => return e.into(),
        };
        let path = match arguments[1].get_string() {
            Ok(v) => v,
            Err(e) => return e.into(),
        };
        match jsonpath_lib::select(&json, &path)
            .map_err(|e| PiperError::InvalidJsonPath(format!("{}", e)))
        {
            Ok(v) => v,
            Err(e) => return e.into(),
        }
        .into_iter()
        .map(Clone::clone)
        .collect()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn jp() {
        let s = r#"{
            "a" : [
                {
                    "b" : [1, 2]
                }
            ]
        }"#;
        let v: serde_json::Value = serde_json::from_str(s).unwrap();
        let path = "$.a[*].c";
        let ret = jsonpath_lib::select(&v, path).unwrap();
        println!("{:?}", ret);
    }
}
