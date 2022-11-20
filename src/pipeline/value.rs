#![allow(dead_code, unused_variables)]

use std::{borrow::Cow, collections::HashMap, fmt::Display};

use serde_json::Number;

use super::PiperError;

/**
 * The type of a value
 */
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueType {
    Null,
    Bool,
    Int,
    Long,
    Float,
    Double,
    String,
    Array,
    Object,
    /**
     * Dynamic means the value is polymorphic, and can be any of the above types.
     */
    Dynamic,
    /**
     * Error means this value is an error.
     */
    Error,
}

impl ValueType {
    /**
     * True if the value type is numeric, including int, long, float, and double.
     */
    pub fn is_numeric(&self) -> bool {
        match self {
            ValueType::Int | ValueType::Long | ValueType::Float | ValueType::Double => true,
            _ => false,
        }
    }
}

impl Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Null => write!(f, "null"),
            ValueType::Bool => write!(f, "bool"),
            ValueType::Int => write!(f, "int"),
            ValueType::Long => write!(f, "long"),
            ValueType::Float => write!(f, "float"),
            ValueType::Double => write!(f, "double"),
            ValueType::String => write!(f, "string"),
            ValueType::Array => write!(f, "array"),
            ValueType::Object => write!(f, "object"),
            ValueType::Dynamic => write!(f, "dynamic"),
            ValueType::Error => write!(f, "error"),
        }
    }
}

/**
 * Value is the type of a value in the pipeline.
 */
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(Cow<'static, str>),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
    Error(PiperError),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(l0), Self::Bool(r0)) => l0 == r0,
            (Self::Int(l0), Self::Int(r0)) => l0 == r0,
            (Self::Long(l0), Self::Long(r0)) => l0 == r0,
            (Self::Float(l0), Self::Float(r0)) => l0 == r0,
            (Self::Double(l0), Self::Double(r0)) => l0 == r0,
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            (Self::Array(l0), Self::Array(r0)) => l0 == r0,
            (Self::Object(l0), Self::Object(r0)) => l0 == r0,
            (Self::Error(l0), Self::Error(r0)) => false,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl Into<serde_json::Value> for Value {
    fn into(self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(v) => serde_json::Value::Bool(v),
            Value::Int(v) => serde_json::Value::Number(v.into()),
            Value::Long(v) => serde_json::Value::Number(v.into()),
            Value::Float(v) => serde_json::Value::Number(Number::from_f64(v as f64).unwrap()),
            Value::Double(v) => serde_json::Value::Number(Number::from_f64(v).unwrap()),
            Value::String(v) => serde_json::Value::String(v.into()),
            Value::Array(v) => serde_json::Value::Array(v.into_iter().map(|x| x.into()).collect()),
            Value::Object(v) => {
                serde_json::Value::Object(v.into_iter().map(|(k, v)| (k, v.into())).collect())
            }
            Value::Error(e) => serde_json::Value::Null,
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(v) => v.into(),
            serde_json::Value::Number(v) => {
                if v.is_u64() {
                    v.as_u64().unwrap().into()
                } else if v.is_i64() {
                    v.as_i64().unwrap().into()
                } else {
                    v.as_f64().unwrap().into()
                }
            }
            serde_json::Value::String(v) => v.into(),
            serde_json::Value::Array(v) => Self::Array(v.into_iter().map(|v| v.into()).collect()),
            serde_json::Value::Object(v) => {
                Self::Object(v.into_iter().map(|(k, v)| (k, v.into())).collect())
            }
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::Int(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Value::Int(value as i32)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Long(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::Long(value as i64)
    }
}

impl From<isize> for Value {
    fn from(value: isize) -> Self {
        Value::Long(value as i64)
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        Value::Long(value as i64)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Double(value)
    }
}

impl From<Cow<'static, str>> for Value {
    fn from(value: Cow<'static, str>) -> Self {
        Value::String(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value.into())
    }
}

impl From<&'static str> for Value {
    fn from(value: &'static str) -> Self {
        Value::String(value.into())
    }
}

impl From<PiperError> for Value {
    fn from(value: PiperError) -> Self {
        Value::Error(value)
    }
}

impl<T> From<Result<T, PiperError>> for Value
where
    T: Into<Value>,
{
    fn from(value: Result<T, PiperError>) -> Self {
        match value {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl<T> From<Vec<T>> for Value
where
    T: Into<Value>,
{
    fn from(value: Vec<T>) -> Self {
        Value::Array(value.into_iter().map(|v| v.into()).collect())
    }
}

impl<T> From<HashMap<String, T>> for Value
where
    T: Into<Value>,
{
    fn from(value: HashMap<String, T>) -> Self {
        Value::Object(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl Value {
    /**
     * Get the type of the value
     */
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Bool(_) => ValueType::Bool,
            Value::Int(_) => ValueType::Int,
            Value::Long(_) => ValueType::Long,
            Value::Float(_) => ValueType::Float,
            Value::Double(_) => ValueType::Double,
            Value::String(_) => ValueType::String,
            Value::Array(_) => ValueType::Array,
            Value::Object(_) => ValueType::Object,
            Value::Error(_) => ValueType::Error,
        }
    }

    /**
     * True if the value is null
     */
    pub fn is_null(&self) -> bool {
        match self {
            Value::Null => true,
            _ => false,
        }
    }

    /**
     * True if the value is null
     */
    pub fn is_error(&self) -> bool {
        match self {
            Value::Error(_) => true,
            _ => false,
        }
    }

    /**
     * Get the bool value, if the value is not a bool, return PiperError::InvalidValueType
     */
    pub fn get_bool(&self) -> Result<bool, PiperError> {
        match self {
            Value::Bool(b) => Ok(*b),
            Value::Error(e) => Err(e.clone())?,
            _ => Err(PiperError::InvalidValueType(
                self.value_type(),
                ValueType::Bool,
            )),
        }
    }

    /**
     * Get the int value, any other numeric types will be automatically converted
     * return PiperError::InvalidValueType in any other cases
     */
    pub fn get_int(&self) -> Result<i32, PiperError> {
        match self {
            Value::Int(v) => Ok(*v),
            Value::Long(v) => Ok(*v as i32),
            Value::Float(v) => Ok(*v as i32),
            Value::Double(v) => Ok(*v as i32),
            _ => Err(PiperError::InvalidValueType(
                self.value_type(),
                ValueType::Int,
            )),
        }
    }

    /**
     * Get the long value, any other numeric types will be automatically converted
     * return PiperError::InvalidValueType in any other cases
     */
    pub fn get_long(&self) -> Result<i64, PiperError> {
        match self {
            Value::Int(v) => Ok(*v as i64),
            Value::Long(v) => Ok(*v as i64),
            Value::Float(v) => Ok(*v as i64),
            Value::Double(v) => Ok(*v as i64),
            _ => Err(PiperError::InvalidValueType(
                self.value_type(),
                ValueType::Long,
            )),
        }
    }

    /**
     * Get the float value, any other numeric types will be automatically converted
     * return PiperError::InvalidValueType in any other cases
     */
    pub fn get_float(&self) -> Result<f32, PiperError> {
        match self {
            Value::Int(v) => Ok(*v as f32),
            Value::Long(v) => Ok(*v as f32),
            Value::Float(v) => Ok(*v as f32),
            Value::Double(v) => Ok(*v as f32),
            _ => Err(PiperError::InvalidValueType(
                self.value_type(),
                ValueType::Float,
            )),
        }
    }

    /**
     * Get the double value, any other numeric types will be automatically converted
     * return PiperError::InvalidValueType in any other cases
     */
    pub fn get_double(&self) -> Result<f64, PiperError> {
        match self {
            Value::Int(v) => Ok(*v as f64),
            Value::Long(v) => Ok(*v as f64),
            Value::Float(v) => Ok(*v as f64),
            Value::Double(v) => Ok(*v as f64),
            _ => Err(PiperError::InvalidValueType(
                self.value_type(),
                ValueType::Double,
            )),
        }
    }

    /**
     * Get the string value, if the value is not a string, return PiperError::InvalidValueType
     */
    pub fn get_string(&self) -> Result<Cow<str>, PiperError> {
        match self {
            Value::String(v) => Ok(v.clone()),
            _ => Err(PiperError::InvalidValueType(
                self.value_type(),
                ValueType::String,
            )),
        }
    }

    /**
     * Get the array value, if the value is not an array, return PiperError::InvalidValueType
     */
    pub fn get_array(&self) -> Result<&Vec<Value>, PiperError> {
        match self {
            Value::Array(v) => Ok(v),
            _ => Err(PiperError::InvalidValueType(
                self.value_type(),
                ValueType::Array,
            )),
        }
    }

    /**
     * Get the object value, if the value is not an object, return PiperError::InvalidValueType
     */
    pub fn get_object(&self) -> Result<&HashMap<String, Value>, PiperError> {
        match self {
            Value::Object(v) => Ok(v),
            _ => Err(PiperError::InvalidValueType(
                self.value_type(),
                ValueType::Object,
            )),
        }
    }

    /**
     * Get the object value, if the value is not an object, return PiperError::InvalidValueType
     */
    pub fn get_error(&self) -> Result<(), PiperError> {
        match self {
            Value::Error(e) => Err(e.clone()),
            _ => Err(PiperError::InvalidValueType(
                self.value_type(),
                ValueType::Object,
            )),
        }
    }

    /**
     * Type cast, number types can be auto casted to each others, others are not
     */
    pub fn try_into(self, value_type: ValueType) -> Result<Value, PiperError> {
        // Dynamic means the value could be any type
        if value_type == ValueType::Dynamic {
            return Ok(self);
        }

        return Ok(match self {
            Value::Null => {
                if self.is_null() {
                    Value::Null
                } else {
                    return Err(PiperError::InvalidTypeCast(self.value_type(), value_type));
                }
            }
            Value::Bool(v) => match value_type {
                ValueType::Bool => self,
                _ => Err(PiperError::InvalidTypeCast(self.value_type(), value_type))?,
            },
            Value::Int(v) => match value_type {
                ValueType::Int => (v as i32).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                _ => Err(PiperError::InvalidTypeCast(self.value_type(), value_type))?,
            },
            Value::Long(v) => match value_type {
                ValueType::Int => (v as i32).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                _ => Err(PiperError::InvalidTypeCast(self.value_type(), value_type))?,
            },
            Value::Float(v) => match value_type {
                ValueType::Int => (v as i32).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                _ => Err(PiperError::InvalidTypeCast(self.value_type(), value_type))?,
            },
            Value::Double(v) => match value_type {
                ValueType::Int => (v as i32).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                _ => Err(PiperError::InvalidTypeCast(self.value_type(), value_type))?,
            },
            Value::String(v) => match value_type {
                ValueType::String => v.into(),
                _ => Err(PiperError::InvalidTypeCast(ValueType::String, value_type))?,
            },
            Value::Array(v) => match value_type {
                ValueType::Array => v.into(),
                _ => Err(PiperError::InvalidTypeCast(ValueType::String, value_type))?,
            },
            Value::Object(v) => match value_type {
                ValueType::Object => v.into(),
                _ => Err(PiperError::InvalidTypeCast(ValueType::String, value_type))?,
            },
            Value::Error(e) => Err(e)?,
        });
    }

    /**
     * Type conversion
     */
    pub fn try_convert(self, value_type: ValueType) -> Result<Value, PiperError> {
        if value_type == ValueType::Dynamic {
            return Ok(self);
        }

        if self.value_type() == value_type {
            return Ok(self);
        }

        return Ok(match self {
            Value::Null => false.into(),
            Value::Bool(v) => match value_type {
                ValueType::Bool => self.clone(),
                ValueType::Int => (if v { 1i32 } else { 0i32 }).into(),
                ValueType::Long => (if v { 1i64 } else { 0i64 }).into(),
                ValueType::Float => (if v { 1f32 } else { 0f32 }).into(),
                ValueType::Double => (if v { 1f64 } else { 0f64 }).into(),
                ValueType::String => (if v { "true" } else { "false" }).into(),
                _ => Err(PiperError::InvalidTypeConversion(
                    self.value_type(),
                    value_type,
                ))?,
            },
            Value::Int(v) => match value_type {
                ValueType::Bool => (v != 0).into(),
                ValueType::Int => (v).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                ValueType::String => Cow::from(v.to_string()).into(),
                _ => Err(PiperError::InvalidTypeConversion(
                    self.value_type(),
                    value_type,
                ))?,
            },
            Value::Long(v) => match value_type {
                ValueType::Bool => (v != 0).into(),
                ValueType::Int => (v as i32).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                ValueType::String => Cow::from(v.to_string()).into(),
                _ => Err(PiperError::InvalidTypeConversion(
                    self.value_type(),
                    value_type,
                ))?,
            },
            Value::Float(v) => match value_type {
                ValueType::Bool => (v != 0f32).into(),
                ValueType::Int => (v as i32).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                ValueType::String => Cow::from(v.to_string()).into(),
                _ => Err(PiperError::InvalidTypeConversion(
                    self.value_type(),
                    value_type,
                ))?,
            },
            Value::Double(v) => match value_type {
                ValueType::Bool => (v != 0f64).into(),
                ValueType::Int => (v as i32).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                ValueType::String => Cow::from(v.to_string()).into(),
                _ => Err(PiperError::InvalidTypeConversion(
                    self.value_type(),
                    value_type,
                ))?,
            },
            Value::String(v) => match value_type {
                ValueType::Bool => (v == "true").into(),
                ValueType::Int => v
                    .parse::<i32>()
                    .map_err(|_| PiperError::FormatError(v.to_string(), value_type))?
                    .into(),
                ValueType::Long => v
                    .parse::<i32>()
                    .map_err(|_| PiperError::FormatError(v.to_string(), value_type))?
                    .into(),
                ValueType::Float => v
                    .parse::<i32>()
                    .map_err(|_| PiperError::FormatError(v.to_string(), value_type))?
                    .into(),
                ValueType::Double => v
                    .parse::<i32>()
                    .map_err(|_| PiperError::FormatError(v.to_string(), value_type))?
                    .into(),
                ValueType::String => (v.to_string()).into(),
                _ => Err(PiperError::InvalidTypeConversion(
                    ValueType::String,
                    value_type,
                ))?,
            },
            Value::Array(v) => match value_type {
                ValueType::Bool => (v.len() > 0).into(),
                ValueType::Array => v.clone().into(),
                _ => Err(PiperError::InvalidTypeConversion(
                    ValueType::Array,
                    value_type,
                ))?,
            },
            Value::Object(v) => match value_type {
                ValueType::Bool => (v.len() > 0).into(),
                ValueType::Object => v.clone().into(),
                _ => Err(PiperError::InvalidTypeConversion(
                    ValueType::Object,
                    value_type,
                ))?,
            },
            Value::Error(e) => Err(e)?,
        });
    }

    pub fn dump(&self) -> String {
        // TODO: String escape
        match self {
            Value::Null => "null".to_string(),
            Value::Bool(v) => v.to_string(),
            Value::Int(v) => v.to_string(),
            Value::Long(v) => v.to_string(),
            Value::Float(v) => v.to_string(),
            Value::Double(v) => v.to_string(),
            Value::String(v) => format!("\"{}\"", v),
            Value::Array(v) => {
                let mut s = "[".to_string();
                for (i, e) in v.iter().enumerate() {
                    if i > 0 {
                        s.push_str(", ");
                    }
                    s.push_str(&e.dump());
                }
                s.push_str("]");
                s
            }
            Value::Object(v) => {
                let mut s = "{".to_string();
                for (i, (k, e)) in v.iter().enumerate() {
                    if i > 0 {
                        s.push_str(", ");
                    }
                    s.push_str(&format!("{}: {}", k, e.dump()));
                }
                s.push_str("}");
                s
            }
            Value::Error(e) => format!("{:?}", e),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Int(x), Value::Int(y)) => x.partial_cmp(y),
            (Value::Int(x), Value::Long(y)) => (*x as i64).partial_cmp(y),
            (Value::Int(x), Value::Float(y)) => (*x as f32).partial_cmp(y),
            (Value::Int(x), Value::Double(y)) => (*x as f64).partial_cmp(y),

            (Value::Long(x), Value::Int(y)) => x.partial_cmp(&(*y as i64)),
            (Value::Long(x), Value::Long(y)) => x.partial_cmp(y),
            (Value::Long(x), Value::Float(y)) => (*x as f64).partial_cmp(&(*y as f64)),
            (Value::Long(x), Value::Double(y)) => (*x as f64).partial_cmp(y),

            (Value::Float(x), Value::Int(y)) => x.partial_cmp(&(*y as f32)),
            (Value::Float(x), Value::Long(y)) => (*x as f64).partial_cmp(&(*y as f64)),
            (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
            (Value::Float(x), Value::Double(y)) => (*x as f64).partial_cmp(y),

            (Value::Double(x), Value::Int(y)) => x.partial_cmp(&(*y as f64)),
            (Value::Double(x), Value::Long(y)) => x.partial_cmp(&(*y as f64)),
            (Value::Double(x), Value::Float(y)) => x.partial_cmp(&(*y as f64)),
            (Value::Double(x), Value::Double(y)) => x.partial_cmp(y),

            (Value::String(x), Value::String(y)) => x.partial_cmp(y),

            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn value_conv() {
        use super::*;
        let v = Value::Int(1);
        assert_eq!(
            v.clone()
                .try_convert(ValueType::Int)
                .unwrap()
                .get_int()
                .unwrap(),
            1i32
        );
        assert_eq!(
            v.clone()
                .try_convert(ValueType::Long)
                .unwrap()
                .get_long()
                .unwrap(),
            1i64
        );
        assert_eq!(
            v.clone()
                .try_convert(ValueType::Float)
                .unwrap()
                .get_float()
                .unwrap(),
            1f32
        );
        assert_eq!(
            v.clone()
                .try_convert(ValueType::Double)
                .unwrap()
                .get_double()
                .unwrap(),
            1f64
        );
        assert_eq!(
            v.clone()
                .try_convert(ValueType::Bool)
                .unwrap()
                .get_bool()
                .unwrap(),
            true
        );
        assert_eq!(
            v.clone()
                .try_convert(ValueType::String)
                .unwrap()
                .get_string()
                .unwrap(),
            "1"
        );
        assert!(v.clone().try_convert(ValueType::Array).is_err());
        assert!(v.clone().try_convert(ValueType::Object).is_err());
    }
}
