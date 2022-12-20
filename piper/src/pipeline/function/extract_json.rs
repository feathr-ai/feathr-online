use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Clone, Debug)]
pub struct ExtractJsonObject;

impl Function for ExtractJsonObject {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 2 {
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
        if argument_types.len() != 2 {
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
    use crate::{Function, Value, ValueType};

    #[test]
    fn test_extract_json_object() {
        let v = Value::String(
            r#"{
            "a": {
                "b" : [1, 2]
            }
        }"#
            .into(),
        );
        let extract_json_object = super::ExtractJsonObject;
        assert!(extract_json_object
            .get_output_type(&[ValueType::String, ValueType::String, ValueType::String])
            .is_err());
        assert!(extract_json_object
            .get_output_type(&[ValueType::String, ValueType::String])
            .is_ok());
        assert!(extract_json_object
            .eval(vec![Value::String("{".into()), Value::String("$.a".into())])
            .is_error());
        assert!(extract_json_object
            .eval(vec![Value::String("{}".into()), Value::Bool(true)])
            .is_error());
        assert!(extract_json_object
            .eval(vec![
                Value::String("{}".into()),
                Value::String(".....".into())
            ])
            .is_error());
        assert_eq!(
            extract_json_object.eval(vec![v, Value::String("$.a".into())]),
            Value::Object(
                vec![(
                    "b".into(),
                    Value::Array(vec![Value::Long(1), Value::Long(2)])
                )]
                .into_iter()
                .collect()
            )
        );
    }

    #[test]
    fn test_extract_json_array() {
        let v = Value::String(
            r#"{
            "a": 1,
            "b": 2,
            "c": 3
        }"#
            .into(),
        );
        let extract_json_array = super::ExtractJsonArray;
        assert!(extract_json_array
            .get_output_type(&[ValueType::String, ValueType::String, ValueType::String])
            .is_err());
        assert!(extract_json_array
            .get_output_type(&[ValueType::String, ValueType::String])
            .is_ok());
        assert!(extract_json_array
            .eval(vec![Value::String("{".into()), Value::String("$.a".into())])
            .is_error());
        assert!(extract_json_array
            .eval(vec![Value::String("{}".into()), Value::Bool(true)])
            .is_error());
        assert!(extract_json_array
            .eval(vec![
                Value::String("{}".into()),
                Value::String(".....".into())
            ])
            .is_error());
        assert_eq!(
            extract_json_array.eval(vec![v, Value::String("$.*".into())]),
            Value::Array(vec![Value::Long(1), Value::Long(2), Value::Long(3)])
        );
    }
}
