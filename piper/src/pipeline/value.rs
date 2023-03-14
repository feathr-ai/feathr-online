use std::{borrow::Cow, collections::HashMap, fmt::Display};

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde_json::Number;

use super::PiperError;

// These are the default formats used by SparkSQL
const DEFAULT_DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";
const DEFAULT_DATE_FORMAT: &str = "%Y-%m-%d";

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
    DateTime,
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
        matches!(
            self,
            ValueType::Int | ValueType::Long | ValueType::Float | ValueType::Double
        )
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
            ValueType::DateTime => write!(f, "datetime"),
            ValueType::Dynamic => write!(f, "dynamic"),
            ValueType::Error => write!(f, "error"),
        }
    }
}

/**
 * Get ValueType from a type, mainly used for expression type inference.
 */
pub trait ValueTypeOf {
    /**
     * Get the ValueType of this type
     */
    fn value_type() -> ValueType;
}

impl ValueTypeOf for Value {
    fn value_type() -> ValueType {
        ValueType::Dynamic
    }
}

impl ValueTypeOf for () {
    fn value_type() -> ValueType {
        ValueType::Null
    }
}

impl ValueTypeOf for bool {
    fn value_type() -> ValueType {
        ValueType::Bool
    }
}

impl ValueTypeOf for i32 {
    fn value_type() -> ValueType {
        ValueType::Int
    }
}

impl ValueTypeOf for u32 {
    fn value_type() -> ValueType {
        ValueType::Int
    }
}

impl ValueTypeOf for i64 {
    fn value_type() -> ValueType {
        ValueType::Long
    }
}

impl ValueTypeOf for u64 {
    fn value_type() -> ValueType {
        ValueType::Long
    }
}

impl ValueTypeOf for isize {
    fn value_type() -> ValueType {
        ValueType::Long
    }
}

impl ValueTypeOf for usize {
    fn value_type() -> ValueType {
        ValueType::Long
    }
}

impl ValueTypeOf for f32 {
    fn value_type() -> ValueType {
        ValueType::Float
    }
}

impl ValueTypeOf for f64 {
    fn value_type() -> ValueType {
        ValueType::Double
    }
}

impl ValueTypeOf for String {
    fn value_type() -> ValueType {
        ValueType::String
    }
}

impl<'a> ValueTypeOf for Cow<'a, str> {
    fn value_type() -> ValueType {
        ValueType::String
    }
}

impl<'a> ValueTypeOf for &'a str {
    fn value_type() -> ValueType {
        ValueType::String
    }
}

impl<Tz> ValueTypeOf for DateTime<Tz>
where
    Tz: TimeZone,
{
    fn value_type() -> ValueType {
        ValueType::DateTime
    }
}

impl ValueTypeOf for NaiveDate {
    fn value_type() -> ValueType {
        ValueType::DateTime
    }
}

impl ValueTypeOf for NaiveDateTime {
    fn value_type() -> ValueType {
        ValueType::DateTime
    }
}

impl ValueTypeOf for PiperError {
    fn value_type() -> ValueType {
        ValueType::Error
    }
}

impl<T> ValueTypeOf for Vec<T>
where
    T: ValueTypeOf,
{
    fn value_type() -> ValueType {
        ValueType::Array
    }
}

impl<T> ValueTypeOf for HashMap<String, T>
where
    T: ValueTypeOf,
{
    fn value_type() -> ValueType {
        ValueType::Object
    }
}

impl<T> ValueTypeOf for Option<T>
where
    T: ValueTypeOf,
{
    fn value_type() -> ValueType {
        T::value_type()
    }
}

impl<T, E> ValueTypeOf for Result<T, E>
where
    T: ValueTypeOf,
{
    fn value_type() -> ValueType {
        T::value_type()
    }
}

/**
 * Value is the type of a value in the pipeline.
 */
#[derive(Debug, Default, Clone)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(Cow<'static, str>),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
    DateTime(DateTime<Utc>),
    Error(PiperError),
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self {
            Self::Bool(v) => v.hash(state),
            Self::Int(v) => v.hash(state),
            Self::Long(v) => v.hash(state),
            Self::Float(v) => v.to_bits().hash(state),
            Self::Double(v) => v.to_bits().hash(state),
            Self::String(v) => v.hash(state),
            Self::Array(v) => v.hash(state),
            Self::DateTime(v) => v.timestamp().hash(state),
            _ => core::mem::discriminant(self).hash(state),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,

            (Self::Bool(l0), Self::Bool(r0)) => l0 == r0,

            (Self::Int(l0), Self::Int(r0)) => l0 == r0,
            (Self::Int(l0), Self::Long(r0)) => *l0 as i64 == *r0,
            (Self::Int(l0), Self::Float(r0)) => *l0 as f64 == *r0 as f64,
            (Self::Int(l0), Self::Double(r0)) => *l0 as f64 == *r0,

            (Self::Long(l0), Self::Int(r0)) => *l0 == *r0 as i64,
            (Self::Long(l0), Self::Long(r0)) => l0 == r0,
            (Self::Long(l0), Self::Float(r0)) => *l0 as f64 == *r0 as f64,
            (Self::Long(l0), Self::Double(r0)) => *l0 as f64 == *r0,

            (Self::Float(l0), Self::Int(r0)) => *l0 as f64 == *r0 as f64,
            (Self::Float(l0), Self::Long(r0)) => *l0 as f64 == *r0 as f64,
            (Self::Float(l0), Self::Float(r0)) => l0 == r0,
            (Self::Float(l0), Self::Double(r0)) => *l0 as f64 == *r0,

            (Self::Double(l0), Self::Int(r0)) => *l0 == *r0 as f64,
            (Self::Double(l0), Self::Long(r0)) => *l0 == *r0 as f64,
            (Self::Double(l0), Self::Float(r0)) => *l0 == *r0 as f64,
            (Self::Double(l0), Self::Double(r0)) => l0 == r0,

            (Self::String(l0), Self::String(r0)) => l0 == r0,
            (Self::Array(l0), Self::Array(r0)) => l0 == r0,
            (Self::Object(l0), Self::Object(r0)) => l0 == r0,

            (Self::DateTime(l0), Self::DateTime(r0)) => l0 == r0,

            (Self::Error(l0), Self::Error(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
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
            (Value::DateTime(x), Value::DateTime(y)) => x.partial_cmp(y),
            (Self::DateTime(x), Self::String(y)) => x.partial_cmp(&match str_to_datetime(y) {
                Ok(dt) => dt,
                Err(_) => return None,
            }),
            (Self::String(x), Self::DateTime(y)) => match str_to_datetime(x) {
                Ok(dt) => dt,
                Err(_) => return None,
            }
            .partial_cmp(y),

            _ => None,
        }
    }
}

pub trait IntoValue {
    fn into_value(self) -> Value;
}

impl<T> IntoValue for T
where
    Value: From<T>,
{
    fn into_value(self) -> Value {
        Value::from(self)
    }
}

impl From<Value> for serde_json::Value {
    fn from(val: Value) -> Self {
        match val {
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
            Value::DateTime(v) => {
                serde_json::Value::String(v.format(DEFAULT_DATETIME_FORMAT).to_string())
            }
            Value::Error(_) => serde_json::Value::Null,
        }
    }
}

impl From<Value> for Result<Value, PiperError> {
    fn from(val: Value) -> Self {
        match val {
            Value::Error(e) => Err(e),
            _ => Ok(val),
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

impl From<DateTime<Utc>> for Value {
    fn from(value: DateTime<Utc>) -> Self {
        Value::DateTime(value)
    }
}

impl From<NaiveDate> for Value {
    fn from(value: NaiveDate) -> Self {
        Value::DateTime(
            Utc.from_local_datetime(&value.and_hms_opt(0, 0, 0).unwrap())
                .unwrap(),
        )
    }
}

impl From<NaiveDateTime> for Value {
    fn from(value: NaiveDateTime) -> Self {
        Value::DateTime(Utc.from_local_datetime(&value).unwrap())
    }
}

impl From<PiperError> for Value {
    fn from(value: PiperError) -> Self {
        Value::Error(value)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

impl<T, E> From<Result<T, E>> for Value
where
    T: Into<Value>,
    E: Into<PiperError>,
{
    fn from(value: Result<T, E>) -> Self {
        match value {
            Ok(v) => v.into(),
            Err(e) => Value::Error(e.into()),
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

impl<T> From<&T> for Value
where
    T: Into<Value> + Clone,
{
    fn from(v: &T) -> Self {
        v.clone().into()
    }
}

impl<T> FromIterator<T> for Value
where
    T: Into<Value>,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Value::Array(iter.into_iter().map(|v| v.into()).collect())
    }
}

macro_rules! impl_from_for_result {
    ($t:ty) => {
        impl From<Value> for Result<$t, PiperError> {
            fn from(value: Value) -> Result<$t, PiperError> {
                <$t>::try_from(value)
            }
        }
    };
}
impl_from_for_result!(bool);
impl_from_for_result!(i32);
impl_from_for_result!(u32);
impl_from_for_result!(i64);
impl_from_for_result!(u64);
impl_from_for_result!(isize);
impl_from_for_result!(usize);
impl_from_for_result!(f32);
impl_from_for_result!(f64);
impl_from_for_result!(String);
impl_from_for_result!(DateTime<Utc>);
impl_from_for_result!(NaiveDate);
impl_from_for_result!(NaiveDateTime);

impl<T> From<Value> for Result<Vec<T>, PiperError>
where
    T: TryFrom<Value, Error = PiperError>,
{
    fn from(value: Value) -> Result<Vec<T>, PiperError> {
        <Vec<T>>::try_from(value)
    }
}

impl<T> From<Value> for Result<HashMap<String, T>, PiperError>
where
    T: TryFrom<Value, Error = PiperError>,
{
    fn from(value: Value) -> Result<HashMap<String, T>, PiperError> {
        <HashMap<String, T>>::try_from(value)
    }
}

impl TryFrom<Value> for bool {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<bool, PiperError> {
        value.get_bool()
    }
}

impl TryFrom<Value> for i32 {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<i32, PiperError> {
        value.get_int()
    }
}

impl TryFrom<Value> for u32 {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<u32, PiperError> {
        value.get_int().map(|v| v as u32)
    }
}

impl TryFrom<Value> for i64 {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<i64, PiperError> {
        value.get_long()
    }
}

impl TryFrom<Value> for u64 {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<u64, PiperError> {
        value.get_long().map(|v| v as u64)
    }
}

impl TryFrom<Value> for isize {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<isize, PiperError> {
        value.get_long().map(|v| v as isize)
    }
}

impl TryFrom<Value> for usize {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<usize, PiperError> {
        value.get_long().map(|v| v as usize)
    }
}

impl TryFrom<Value> for f32 {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<f32, PiperError> {
        value.get_float()
    }
}

impl TryFrom<Value> for f64 {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<f64, PiperError> {
        value.get_double()
    }
}

impl TryFrom<Value> for String {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<String, PiperError> {
        match value {
            Value::String(s) => Ok(s.into()),
            _ => Err(PiperError::InvalidTypeCast(
                value.value_type(),
                ValueType::Array,
            )),
        }
    }
}

impl TryFrom<Value> for DateTime<Utc> {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<DateTime<Utc>, PiperError> {
        value.get_datetime()
    }
}

impl TryFrom<Value> for NaiveDate {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<NaiveDate, PiperError> {
        value.get_datetime().map(|d| d.naive_utc().date())
    }
}

impl TryFrom<Value> for NaiveDateTime {
    type Error = PiperError;

    fn try_from(value: Value) -> Result<NaiveDateTime, PiperError> {
        value.get_datetime().map(|d| d.naive_utc())
    }
}

impl<T, E> TryFrom<Value> for Vec<T>
where
    T: TryFrom<Value, Error = E>,
    E: Into<PiperError>,
{
    type Error = PiperError;

    fn try_from(value: Value) -> Result<Vec<T>, PiperError> {
        match value {
            Value::Array(a) => a
                .into_iter()
                .map(|v| T::try_from(v))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.into()),
            _ => Err(PiperError::InvalidTypeCast(
                value.value_type(),
                ValueType::Array,
            )),
        }
    }
}

impl<T, E> TryFrom<Value> for HashMap<String, T>
where
    T: TryFrom<Value, Error = E>,
    E: Into<PiperError>,
{
    type Error = PiperError;

    fn try_from(value: Value) -> Result<HashMap<String, T>, PiperError> {
        match value {
            Value::Object(o) => o
                .into_iter()
                .map(|(k, v)| T::try_from(v).map(|v| (k, v)))
                .collect::<Result<HashMap<_, _>, _>>()
                .map_err(|e| e.into()),
            _ => Err(PiperError::InvalidTypeCast(
                value.value_type(),
                ValueType::Object,
            )),
        }
    }
}

macro_rules! impl_try_from_for_option {
    ($t:ty) => {
        impl TryFrom<Value> for Option<$t> {
            type Error = PiperError;

            fn try_from(value: Value) -> Result<Option<$t>, PiperError> {
                if value.is_null() {
                    return Ok(None);
                }
                <$t>::try_from(value).map(Some)
            }
        }
    };
}

impl_try_from_for_option!(bool);
impl_try_from_for_option!(i32);
impl_try_from_for_option!(u32);
impl_try_from_for_option!(i64);
impl_try_from_for_option!(u64);
impl_try_from_for_option!(isize);
impl_try_from_for_option!(usize);
impl_try_from_for_option!(f32);
impl_try_from_for_option!(f64);
impl_try_from_for_option!(String);
impl_try_from_for_option!(DateTime<Utc>);
impl_try_from_for_option!(NaiveDate);
impl_try_from_for_option!(NaiveDateTime);

impl<T, E> TryFrom<Value> for Option<Vec<T>>
where
    T: TryFrom<Value, Error = E>,
    E: Into<PiperError>,
{
    type Error = PiperError;

    fn try_from(value: Value) -> Result<Option<Vec<T>>, PiperError> {
        if value.is_null() {
            return Ok(None);
        }
        match value {
            Value::Array(a) => a
                .into_iter()
                .map(|v| T::try_from(v))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.into()),
            _ => Err(PiperError::InvalidTypeCast(
                value.value_type(),
                ValueType::Array,
            )),
        }
        .map(Some)
    }
}

impl<T, E> TryFrom<Value> for Option<HashMap<String, T>>
where
    T: TryFrom<Value, Error = E>,
    E: Into<PiperError>,
{
    type Error = PiperError;

    fn try_from(value: Value) -> Result<Option<HashMap<String, T>>, PiperError> {
        if value.is_null() {
            return Ok(None);
        }
        match value {
            Value::Object(o) => o
                .into_iter()
                .map(|(k, v)| T::try_from(v).map(|v| (k, v)))
                .collect::<Result<HashMap<_, _>, _>>()
                .map_err(|e| e.into()),
            _ => Err(PiperError::InvalidTypeCast(
                value.value_type(),
                ValueType::Object,
            )),
        }
        .map(Some)
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
            Value::DateTime(_) => ValueType::DateTime,
            Value::Error(_) => ValueType::Error,
        }
    }

    /**
     * True if the value is null
     */
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /**
     * True if the value is null
     */
    pub fn is_error(&self) -> bool {
        matches!(self, Value::Error(_))
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
            Value::Error(e) => Err(e.clone())?,
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
            Value::Long(v) => Ok(*v),
            Value::Float(v) => Ok(*v as i64),
            Value::Double(v) => Ok(*v as i64),
            Value::Error(e) => Err(e.clone())?,
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
            Value::Float(v) => Ok(*v),
            Value::Double(v) => Ok(*v as f32),
            Value::Error(e) => Err(e.clone())?,
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
            Value::Double(v) => Ok(*v),
            Value::Error(e) => Err(e.clone())?,
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
            Value::DateTime(v) => Ok(v.format(DEFAULT_DATETIME_FORMAT).to_string().into()),
            Value::Error(e) => Err(e.clone())?,
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
            Value::Error(e) => Err(e.clone())?,
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
            Value::Error(e) => Err(e.clone())?,
            _ => Err(PiperError::InvalidValueType(
                self.value_type(),
                ValueType::Object,
            )),
        }
    }

    /**
     * Get the datetime value, if the value is not a datetime, return PiperError::InvalidValueType
     */
    pub fn get_datetime(&self) -> Result<DateTime<Utc>, PiperError> {
        match self {
            Value::String(v) => str_to_datetime(v.as_ref()),
            Value::DateTime(v) => Ok(*v),
            Value::Error(e) => Err(e.clone())?,
            _ => Err(PiperError::InvalidValueType(
                self.value_type(),
                ValueType::DateTime,
            )),
        }
    }

    /**
     * Get the error value, if the value is not an error, return PiperError::InvalidValueType
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
     * Type cast, number types can be auto casted to each others, string can be casted to datetime and vice versa.
     */
    pub fn cast_to(self, value_type: ValueType) -> Value {
        // Dynamic means the value could be any type
        if value_type == ValueType::Dynamic {
            return self;
        }

        // If the value is already the same type, return the value directly
        if self.value_type() == value_type {
            return self;
        }

        match self {
            Value::Null => Value::Null,
            Value::Bool(_) => {
                Value::Error(PiperError::InvalidTypeCast(self.value_type(), value_type))
            }
            Value::Int(v) => match value_type {
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                _ => Value::Error(PiperError::InvalidTypeCast(self.value_type(), value_type)),
            },
            Value::Long(v) => match value_type {
                ValueType::Int => (v as i32).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                _ => Value::Error(PiperError::InvalidTypeCast(self.value_type(), value_type)),
            },
            Value::Float(v) => match value_type {
                ValueType::Int => (v as i32).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Double => (v as f64).into(),
                _ => Value::Error(PiperError::InvalidTypeCast(self.value_type(), value_type)),
            },
            Value::Double(v) => match value_type {
                ValueType::Int => (v as i32).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                _ => Value::Error(PiperError::InvalidTypeCast(self.value_type(), value_type)),
            },
            Value::String(v) => match value_type {
                ValueType::DateTime => str_to_datetime(v.as_ref()).into(),
                _ => Value::Error(PiperError::InvalidTypeCast(ValueType::String, value_type)),
            },
            Value::Array(_) => {
                Value::Error(PiperError::InvalidTypeCast(ValueType::Array, value_type))
            }
            Value::Object(_) => {
                Value::Error(PiperError::InvalidTypeCast(ValueType::Object, value_type))
            }
            Value::DateTime(v) => match value_type {
                ValueType::String => v.format(DEFAULT_DATETIME_FORMAT).to_string().into(),
                _ => Value::Error(PiperError::InvalidTypeCast(ValueType::DateTime, value_type)),
            },
            Value::Error(e) => Value::Error(e),
        }
    }

    /**
     * Type conversion
     */
    pub fn convert_to(self, value_type: ValueType) -> Value {
        if value_type == ValueType::Dynamic {
            return self;
        }

        // If the value is already the same type, return the value directly
        if self.value_type() == value_type {
            return self;
        }

        match self {
            Value::Null => match value_type {
                ValueType::Bool => false.into(),
                _ => Value::Null,
            },
            Value::Bool(v) => match value_type {
                ValueType::Int => i32::from(v).into(),
                ValueType::Long => i64::from(v).into(),
                ValueType::Float => (if v { 1f32 } else { 0f32 }).into(),
                ValueType::Double => (if v { 1f64 } else { 0f64 }).into(),
                ValueType::String => (if v { "true" } else { "false" }).into(),
                _ => Value::Error(PiperError::InvalidTypeConversion(
                    self.value_type(),
                    value_type,
                )),
            },
            Value::Int(v) => match value_type {
                ValueType::Bool => (v != 0).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                ValueType::String => Cow::from(v.to_string()).into(),
                _ => Value::Error(PiperError::InvalidTypeConversion(
                    self.value_type(),
                    value_type,
                )),
            },
            Value::Long(v) => match value_type {
                ValueType::Bool => (v != 0).into(),
                ValueType::Int => (v as i32).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::Double => (v as f64).into(),
                ValueType::String => Cow::from(v.to_string()).into(),
                _ => Value::Error(PiperError::InvalidTypeConversion(
                    self.value_type(),
                    value_type,
                )),
            },
            Value::Float(v) => match value_type {
                ValueType::Bool => (v != 0f32).into(),
                ValueType::Int => (v as i32).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Double => (v as f64).into(),
                ValueType::String => Cow::from(v.to_string()).into(),
                _ => Value::Error(PiperError::InvalidTypeConversion(
                    self.value_type(),
                    value_type,
                )),
            },
            Value::Double(v) => match value_type {
                ValueType::Bool => (v != 0f64).into(),
                ValueType::Int => (v as i32).into(),
                ValueType::Long => (v as i64).into(),
                ValueType::Float => (v as f32).into(),
                ValueType::String => Cow::from(v.to_string()).into(),
                _ => Value::Error(PiperError::InvalidTypeConversion(
                    self.value_type(),
                    value_type,
                )),
            },
            Value::String(v) => match value_type {
                ValueType::Bool => (v == "true").into(),
                ValueType::Int => v
                    .parse::<i32>()
                    .map_err(|_| PiperError::FormatError(v.to_string(), value_type))
                    .into(),
                ValueType::Long => v
                    .parse::<i32>()
                    .map_err(|_| PiperError::FormatError(v.to_string(), value_type))
                    .into(),
                ValueType::Float => v
                    .parse::<i32>()
                    .map_err(|_| PiperError::FormatError(v.to_string(), value_type))
                    .into(),
                ValueType::Double => v
                    .parse::<i32>()
                    .map_err(|_| PiperError::FormatError(v.to_string(), value_type))
                    .into(),
                ValueType::DateTime => str_to_datetime(v.as_ref()).into(),
                _ => Value::Error(PiperError::InvalidTypeConversion(
                    ValueType::String,
                    value_type,
                )),
            },
            Value::DateTime(v) => match value_type {
                ValueType::String => v.format(DEFAULT_DATETIME_FORMAT).to_string().into(),
                _ => Value::Error(PiperError::InvalidTypeConversion(
                    ValueType::DateTime,
                    value_type,
                )),
            },
            Value::Array(v) => match value_type {
                ValueType::Bool => (!v.is_empty()).into(),
                _ => Value::Error(PiperError::InvalidTypeConversion(
                    ValueType::Array,
                    value_type,
                )),
            },
            Value::Object(v) => match value_type {
                ValueType::Bool => (!v.is_empty()).into(),
                _ => Value::Error(PiperError::InvalidTypeConversion(
                    ValueType::Object,
                    value_type,
                )),
            },
            Value::Error(e) => Value::Error(e),
        }
    }

    /**
     * Dump the value into a string
     */
    pub fn dump(&self) -> String {
        match self {
            Value::Null => "null".to_string(),
            Value::Bool(v) => v.to_string(),
            Value::Int(v) => v.to_string(),
            Value::Long(v) => v.to_string(),
            Value::Float(v) => v.to_string(),
            Value::Double(v) => v.to_string(),
            Value::String(v) => format!("\"{}\"", escaped(v)),
            Value::DateTime(v) => format!("\"{}\"", v.format(DEFAULT_DATETIME_FORMAT)),
            Value::Array(v) => {
                let mut s = "[".to_string();
                for (i, e) in v.iter().enumerate() {
                    if i > 0 {
                        s.push_str(", ");
                    }
                    s.push_str(&e.dump());
                }
                s.push(']');
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
                s.push('}');
                s
            }
            Value::Error(e) => format!("{e:?}"),
        }
    }
}

fn escaped<T>(s: T) -> String
where
    T: AsRef<str>,
{
    let mut r = String::new();
    for c in s.as_ref().chars() {
        match c {
            '"' => r.push_str("\\\""),
            '\\' => r.push_str("\\\\"),
            '\t' => r.push_str("\\t"),
            '\r' => r.push_str("\\r"),
            '\n' => r.push_str("\\n"),
            _ => r.push(c),
        }
    }
    r
}

fn str_to_datetime(v: &str) -> Result<DateTime<Utc>, PiperError> {
    let dt = if let Ok(dt) = NaiveDateTime::parse_from_str(v, DEFAULT_DATETIME_FORMAT) {
        dt
    } else if let Ok(d) = NaiveDate::parse_from_str(v, DEFAULT_DATE_FORMAT) {
        d.and_hms_opt(0, 0, 0).unwrap()
    } else {
        return Err(PiperError::InvalidTypeCast(
            ValueType::String,
            ValueType::DateTime,
        ));
    };
    Ok(Utc.from_local_datetime(&dt).unwrap())
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use crate::pipeline::{value::str_to_datetime, Value};

    #[test]
    fn test_value_type() {
        assert_eq!(format!("{}", super::ValueType::Null), "null");
        assert_eq!(format!("{}", super::ValueType::Bool), "bool");
        assert_eq!(format!("{}", super::ValueType::Int), "int");
        assert_eq!(format!("{}", super::ValueType::Long), "long");
        assert_eq!(format!("{}", super::ValueType::Float), "float");
        assert_eq!(format!("{}", super::ValueType::Double), "double");
        assert_eq!(format!("{}", super::ValueType::String), "string");
        assert_eq!(format!("{}", super::ValueType::DateTime), "datetime");
        assert_eq!(format!("{}", super::ValueType::Array), "array");
        assert_eq!(format!("{}", super::ValueType::Object), "object");
    }

    #[test]
    fn value_conv() {
        use super::*;
        let v = Value::Int(1);
        assert_eq!(
            v.clone().convert_to(ValueType::Int).get_int().unwrap(),
            1i32
        );
        assert_eq!(
            v.clone().convert_to(ValueType::Long).get_long().unwrap(),
            1i64
        );
        assert_eq!(
            v.clone().convert_to(ValueType::Float).get_float().unwrap(),
            1f32
        );
        assert_eq!(
            v.clone()
                .convert_to(ValueType::Double)
                .get_double()
                .unwrap(),
            1f64
        );
        assert!(v.clone().convert_to(ValueType::Bool).get_bool().unwrap());
        assert_eq!(
            v.clone()
                .convert_to(ValueType::String)
                .get_string()
                .unwrap(),
            "1"
        );
        assert!(v.clone().convert_to(ValueType::Array).is_error());
        assert!(v.convert_to(ValueType::Object).is_error());
    }

    #[test]
    fn datetime_str_cast() {
        // Auto-cast between string and datetime
        let vs: Value = "2022-03-04".to_string().into();
        let vd: Value = NaiveDate::from_ymd_opt(2022, 3, 4).unwrap().into();
        assert_eq!(vs.get_datetime().unwrap(), vd.get_datetime().unwrap());
        assert_eq!(vd.get_string().unwrap(), "2022-03-04 00:00:00");
    }

    #[test]
    fn test_into_value() {
        use super::*;
        assert_eq!(Option::<i32>::None.into_value(), Value::Null);
        assert_eq!(42i32.into_value(), Value::Int(42));
        assert_eq!(42i64.into_value(), Value::Long(42));
        assert_eq!(42f32.into_value(), Value::Float(42f32));
        assert_eq!(42f64.into_value(), Value::Double(42f64));
        assert_eq!("foo".into_value(), Value::String("foo".into()));
        assert_eq!(
            vec![1u32, 2u32, 3u32].into_value(),
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
        assert_eq!(
            str_to_datetime("2022-03-04").into_value(),
            Value::DateTime(str_to_datetime("2022-03-04").unwrap())
        );
    }

    #[test]
    fn test_cmp() {
        assert_eq!(Value::Int(1), Value::Int(1));
        assert_eq!(Value::Int(1), Value::Long(1));
        assert_eq!(Value::Float(1f32), Value::Int(1));
        assert_eq!(Value::Long(1), Value::Double(1f64));

        assert!(Value::Int(1) < Value::Int(2));
        assert!(Value::Long(1) < Value::Int(2));
        assert!(Value::Float(1f32) < Value::Int(2));
        assert!(Value::Int(1) < Value::Double(2f64));

        assert!(Value::Bool(true) != Value::Double(2f64));

        assert_eq!(
            Value::String("2022-03-04".into()).get_datetime().unwrap(),
            Value::DateTime(str_to_datetime("2022-03-04").unwrap())
                .get_datetime()
                .unwrap()
        );

        assert!(
            Value::String("2022-03-01".into()).get_datetime().unwrap()
                < Value::DateTime(str_to_datetime("2022-03-04").unwrap())
                    .get_datetime()
                    .unwrap()
        );

        assert_eq!(
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            Value::Array(vec![Value::Int(1), Value::Int(2)])
        );
    }

    #[test]
    fn test_value() {
        assert_eq!(Value::String("a".into()).dump(), "\"a\"".to_string());
        assert_eq!(Value::String("a\t".into()).dump(), "\"a\\t\"".to_string());
        assert_eq!(Value::Int(10).get_int().unwrap(), 10);
        assert_eq!(Value::Int(10).get_long().unwrap(), 10);
        assert_eq!(Value::Int(10).get_float().unwrap(), 10f32);
        assert_eq!(Value::Int(10).get_double().unwrap(), 10f64);
        assert!(Value::Int(0).get_bool().is_err());
        assert!(!Value::Int(10).is_error());
        assert!(!Value::Int(10).is_null());
        assert!(Value::Null.is_null());
        assert!(!Value::Null.is_error());
    }
}
