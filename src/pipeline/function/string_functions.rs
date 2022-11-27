use tracing::instrument;

use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Debug)]
pub struct SplitFunction;

impl Function for SplitFunction {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 2 {
            return Err(PiperError::ArityError(
                "split".to_string(),
                argument_types.len(),
            ));
        }
        if argument_types[0] != ValueType::String {
            return Err(PiperError::InvalidArgumentType(
                "split".to_string(),
                0,
                argument_types[0],
            ));
        }
        if argument_types[1] != ValueType::String {
            return Err(PiperError::InvalidArgumentType(
                "split".to_string(),
                1,
                argument_types[1],
            ));
        }
        Ok(ValueType::Array)
    }

    #[instrument(level = "trace", skip(self))]
    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() != 2 {
            return Value::Error(PiperError::InvalidArgumentCount(2, arguments.len()));
        }
        let string = match arguments[0].get_string() {
            Ok(string) => string,
            Err(err) => return Value::Error(err),
        };
        let delimiter = match arguments[1].get_string() {
            Ok(string) => string,
            Err(err) => return Value::Error(err),
        };
        let mut result = Vec::new();
        for s in string.split(delimiter.as_ref()) {
            result.push(Value::String(s.to_string().into()));
        }
        Value::Array(result)
    }
}

#[derive(Debug)]
pub struct SubstringFunction;

impl Function for SubstringFunction {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 2 {
            return Err(PiperError::ArityError(
                "substring".to_string(),
                argument_types.len(),
            ));
        }
        if argument_types[0] != ValueType::String {
            return Err(PiperError::InvalidArgumentType(
                "substring".to_string(),
                0,
                argument_types[0],
            ));
        }
        if argument_types[1] != ValueType::Int {
            return Err(PiperError::InvalidArgumentType(
                "substring".to_string(),
                1,
                argument_types[1],
            ));
        }
        if argument_types[2] != ValueType::Int {
            return Err(PiperError::InvalidArgumentType(
                "substring".to_string(),
                2,
                argument_types[2],
            ));
        }
        Ok(ValueType::String)
    }

    #[instrument(level = "trace", skip(self))]
    fn eval(&self, mut arguments: Vec<Value>) -> Value {
        if arguments.len() != 3 {
            return Value::Error(PiperError::InvalidArgumentCount(3, arguments.len()));
        }
        let length = match arguments.remove(2).convert_to(ValueType::Long).get_long() {
            Ok(string) => string,
            Err(err) => return Value::Error(err),
        };
        let start = match arguments.remove(1).convert_to(ValueType::Long).get_long() {
            Ok(string) => string,
            Err(err) => return Value::Error(err),
        };
        let arg0 = arguments.remove(0);
        let string = match arg0.get_string() {
            Ok(string) => string,
            Err(err) => return Value::Error(err),
        };
        let start = if start < 0 {
            string.len() as i64 + start
        } else {
            start
        };
        let length = if length < 0 {
            string.len() as i64 + length - start
        } else {
            length
        };
        Value::String(
            string[start as usize..(start + length) as usize]
                .to_string()
                .into(),
        )
    }
}

pub fn substring_index(string: String, delimiter: String, count: i64) -> String {
    let mut count = count;
    if count >= 0 {
        let mut start = 0;
        let mut end;
        let mut ret_end = 0;
        while count > 0 {
            end = match string[start..].find(&delimiter) {
                Some(index) => start + index,
                None => string.len(),
            };
            ret_end = end;
            start = end + delimiter.len();
            if start >= string.len() {
                ret_end = string.len();
                break;
            }
            if count == 1 {
                break;
            } else {
                ret_end += delimiter.len();
            }
            count -= 1;
        }
        string[..ret_end].to_string()
    } else {
        let mut start = string.len();
        let mut end;
        let mut ret_start = 0;
        while count < 0 {
            end = string[..start].rfind(&delimiter).unwrap_or(0);
            ret_start = end;
            start = end;
            if start == 0 {
                break;
            }
            if count == -1 {
                ret_start += delimiter.len();
                break;
            }
            if ret_start == 0 {
                break;
            }
            count += 1;
        }
        string[ret_start..string.len()].to_string()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_substring_index() {
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), 2),
            "www.apache"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), 3),
            "www.apache.org"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), 4),
            "www.apache.org"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), -1),
            "org"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), -2),
            "apache.org"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), -3),
            "www.apache.org"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), -4),
            "www.apache.org"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), 0),
            ""
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), 1),
            "www"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), 2),
            "www.apache"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), 3),
            "www.apache.org"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), 4),
            "www.apache.org"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), -1),
            "org"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), -2),
            "apache.org"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), -3),
            "www.apache.org"
        );
        assert_eq!(
            super::substring_index("www.apache.org".to_string(), ".".to_string(), -4),
            "www.apache.org"
        );
    }
}