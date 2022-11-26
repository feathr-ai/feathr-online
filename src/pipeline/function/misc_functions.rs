use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Debug)]
pub struct Abs;

impl Function for Abs {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 1 {
            return Err(PiperError::ArityError(
                stringify!($op).to_string(),
                argument_types.len(),
            ));
        }
        if !argument_types[0].is_numeric() {
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

pub struct Concat;

impl Function for Concat {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() < 2 {
            return Err(PiperError::InvalidArgumentCount(2, argument_types.len()));
        }
        let init_type = argument_types[0];
        if init_type != ValueType::String && init_type != ValueType::Array {
            return Err(PiperError::InvalidArgumentType(
                "concat".to_string(),
                0,
                init_type,
            ));
        }
        for (idx, vt) in argument_types.iter().skip(1).enumerate() {
            if *vt != init_type {
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
