use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Clone, Debug)]
pub struct Abs;

impl Function for Abs {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 1 {
            return Err(PiperError::ArityError(
                stringify!($op).to_string(),
                argument_types.len(),
            ));
        }
        if !argument_types[0].is_numeric() && argument_types[0] != ValueType::Dynamic {
            return Err(PiperError::InvalidArgumentType(
                stringify!($op).to_string(),
                0,
                argument_types[0],
            ));
        }
        Ok(argument_types[0])
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() != 1 {
            return Value::Error(PiperError::InvalidArgumentCount(1, arguments.len()));
        }
        match arguments[0] {
            Value::Int(v) => Value::Int(v.abs()),
            Value::Long(v) => Value::Long(v.abs()),
            Value::Float(v) => Value::Float(v.abs()),
            Value::Double(v) => Value::Double(v.abs()),
            _ => unreachable!(),
        }
    }
}

pub fn ascii(s: String) -> Value {
    s.chars().next().map_or(Value::Null, |c| (c as u32).into())
}

#[derive(Clone)]
pub struct Concat;

impl Function for Concat {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() < 2 {
            return Err(PiperError::InvalidArgumentCount(2, argument_types.len()));
        }
        let init_type = argument_types.iter().find(|t| **t != ValueType::Dynamic);
        let init_type = match init_type {
            Some(t) => *t,
            None => return Ok(ValueType::Dynamic),      // All arguments are dynamic
        };
        if init_type != ValueType::String && init_type != ValueType::Array {
            return Err(PiperError::InvalidArgumentType(
                "concat".to_string(),
                0,
                init_type,
            ));
        }
        for (idx, vt) in argument_types.iter().enumerate() {
            if *vt != ValueType::Dynamic && *vt != init_type {
                return Err(PiperError::InvalidArgumentType(
                    "concat".to_string(),
                    idx + 1,
                    *vt,
                ));
            }
        }
        Ok(init_type)
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() < 2 {
            return Value::Error(PiperError::InvalidArgumentCount(2, arguments.len()));
        }
        if let Ok(array) = arguments[0].get_array() {
            // concat array
            let mut result = array.clone();
            for arg in arguments.iter().skip(1) {
                if let Ok(array) = arg.get_array() {
                    result.extend(array.clone());
                } else {
                    return Value::Error(PiperError::InvalidArgumentType(
                        "concat".to_string(),
                        0,
                        ValueType::Array,
                    ));
                }
            }
            result.into()
        } else if let Ok(s) = arguments[0].get_string() {
            // concat string
            let mut s = s.to_string();
            for arg in arguments.iter().skip(1) {
                if let Ok(s2) = arg.get_string() {
                    s.push_str(s2.as_ref());
                } else {
                    return Value::Error(PiperError::InvalidArgumentType(
                        "concat".to_string(),
                        1,
                        arg.value_type(),
                    ));
                }
            }
            s.into()
        } else {
            Value::Error(PiperError::InvalidArgumentType(
                "concat".to_string(),
                0,
                arguments[0].value_type(),
            ))
        }
    }
}

#[derive(Clone)]
pub struct ConcatWs;

impl Function for ConcatWs {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() < 2 {
            return Err(PiperError::InvalidArgumentCount(2, argument_types.len()));
        }
        for (idx, vt) in argument_types.iter().enumerate() {
            if *vt != ValueType::String && *vt != ValueType::Array {
                return Err(PiperError::InvalidArgumentType(
                    "concat_ws".to_string(),
                    idx,
                    *vt,
                ));
            }
        }
        Ok(ValueType::String)
    }
    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() < 2 {
            return Value::Error(PiperError::InvalidArgumentCount(2, arguments.len()));
        }
        if let Ok(sep) = arguments[0].get_string() {
            let mut result = String::new();
            let mut first = true;
            for arg in arguments.iter().skip(1) {
                if let Ok(array) = arg.get_array() {
                    for item in array {
                        if let Ok(s) = item.get_string() {
                            if !first {
                                result.push_str(sep.as_ref());
                            }
                            result.push_str(s.as_ref());
                            first = false;
                        }
                    }
                } else if let Ok(s) = arg.get_string() {
                    if !first {
                        result.push_str(sep.as_ref());
                    }
                    result.push_str(s.as_ref());
                    first = false;
                }
            }
            result.into()
        } else {
            Value::Error(PiperError::InvalidArgumentType(
                "concat_ws".to_string(),
                0,
                arguments[0].value_type(),
            ))
        }
    }
}

pub fn contains(s: Option<String>, substr: Option<String>) -> Value {
    match (s, substr) {
        (Some(s), Some(substr)) => s.contains(&substr).into(),
        _ => Value::Null,
    }
}

#[derive(Clone)]
pub struct Conv;

impl Function for Conv {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 3 {
            return Err(PiperError::InvalidArgumentCount(3, argument_types.len()));
        }
        if argument_types[0] != ValueType::String {
            return Err(PiperError::InvalidArgumentType(
                "conv".to_string(),
                0,
                argument_types[0],
            ));
        }
        if argument_types[1] != ValueType::Int {
            return Err(PiperError::InvalidArgumentType(
                "conv".to_string(),
                1,
                argument_types[1],
            ));
        }
        if argument_types[2] != ValueType::Int {
            return Err(PiperError::InvalidArgumentType(
                "conv".to_string(),
                2,
                argument_types[2],
            ));
        }
        Ok(ValueType::String)
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() != 3 {
            return Value::Error(PiperError::InvalidArgumentCount(3, arguments.len()));
        }
        if let Ok(s) = arguments[0].get_string() {
            if let Ok(from_base) = arguments[1].get_int() {
                if let Ok(to_base) = arguments[2].get_int() {
                    if !(2..=36).contains(&from_base) {
                        return Value::Error(PiperError::InvalidValue(format!(
                            "from_base must be between 2 and 36, got {}",
                            from_base
                        )));
                    }
                    if !(2..=36).contains(&to_base) {
                        return Value::Error(PiperError::InvalidValue(format!(
                            "to_base must be between 2 and 36, got {}",
                            to_base
                        )));
                    }
                    let mut result = String::new();
                    let mut n = u64::from_str_radix(s.as_ref(), from_base as u32).unwrap();
                    while n > 0 {
                        let r = n % to_base as u64;
                        n /= to_base as u64;
                        result.push_str(
                            (if r < 10 {
                                (b'0' + r as u8) as char
                            } else {
                                (b'a' + r as u8 - 10) as char
                            })
                            .to_string()
                            .as_ref(),
                        );
                    }
                    result.into()
                } else {
                    Value::Error(PiperError::InvalidArgumentType(
                        "conv".to_string(),
                        2,
                        arguments[2].value_type(),
                    ))
                }
            } else {
                Value::Error(PiperError::InvalidArgumentType(
                    "conv".to_string(),
                    1,
                    arguments[1].value_type(),
                ))
            }
        } else {
            Value::Error(PiperError::InvalidArgumentType(
                "conv".to_string(),
                0,
                arguments[0].value_type(),
            ))
        }
    }
}


pub fn json_object_keys(json: Option<String>) -> Value {
    match json {
        Some(json) => {
            let mut result: Vec<String> = Vec::new();
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(json.as_ref()) {
                if let Some(map) = json.as_object() {
                    for key in map.keys() {
                        result.push(key.clone());
                    }
                }
            }
            result.into()
        }
        None => Value::Null,
    }
}

pub fn json_array_length(json: Option<String>) -> Value {
    match json {
        Some(json) => {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(json.as_ref()) {
                if let Some(array) = json.as_array() {
                    return array.len().into();
                }
            }
            0.into()
        }
        None => Value::Null,
    }
}

pub fn element_at(container: Value, index: Value) -> Value {
    match container {
        Value::Array(array) => {
            if let Value::Long(index) = index {
                if index >= 0 && index < array.len() as i64 {
                    return array[index as usize].clone();
                }
            }
            Value::Null
        }
        Value::Object(map) => {
            if let Value::String(index) = index {
                if let Some(value) = map.get(index.as_ref()) {
                    return value.clone();
                }
            }
            Value::Null
        }
        _ => Value::Null,
    }
}

pub fn elt(arguments: Vec<Value>) -> Value {
    if arguments.len() < 2 {
        return Value::Error(PiperError::InvalidArgumentCount(2, arguments.len()));
    }
    if let Value::Long(index) = arguments[0] {
        if index >= 0 && index < arguments.len() as i64 {
            return arguments[index as usize + 1].clone();
        }
    }
    Value::Null
}

pub fn slice(array: Vec<Value>, start: i64, end: i64) -> Result<Value, PiperError> {
    let start = if start<0 { array.len() as i64 + start } else { start };
    let start = if start<0 { 0 } else { start as usize };
    let end = if end<0 { array.len() as i64 + end } else { end };
    let end = if end<0 { 0 } else { end as usize };
    if start > end {
        return Err(PiperError::InvalidValue(format!("start ({}) must be less than end ({})", start, end)));
    }
    Ok(Value::Array(array[start..end].to_vec()))
}

pub fn distance(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let lat1 = lat1.to_radians();
    let lng1 = lng1.to_radians();
    let lat2 = lat2.to_radians();
    let lng2 = lng2.to_radians();
    let dlat = lat2 - lat1;
    let dlng = lng2 - lng1;
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    6371.0 * c
}

#[cfg(test)]
mod tests {
    use crate::pipeline::value::IntoValue;

    #[test]
    fn test_slice() {
        use super::*;
        let array = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10].into_value().get_array().unwrap().clone();
        assert_eq!(slice(array.clone(), 0, 5).unwrap(), vec![1i32, 2, 3, 4, 5].into_value());
        assert_eq!(slice(array.clone(), 0, 0).unwrap(), Value::Array(vec![]));
        assert_eq!(slice(array, 0, -1).unwrap(), vec![1i32, 2, 3, 4, 5, 6, 7, 8, 9].into_value());
    }
}