use crate::pipeline::{Value, ValueType};

use super::Function;

pub fn array_contains(array: Vec<Value>, value: Value) -> Value {
    Value::Bool(array.contains(&value))
}

pub fn array_distinct(array: Vec<Value>) -> Value {
    let mut distinct = Vec::new();
    for value in array {
        if !distinct.contains(&value) {
            distinct.push(value);
        }
    }
    Value::Array(distinct)
}

pub fn array_except(array: Vec<Value>, except: Vec<Value>) -> Value {
    let mut result = Vec::new();
    for value in array {
        if !except.contains(&value) {
            result.push(value);
        }
    }
    Value::Array(result)
}

pub fn array_intersect(array: Vec<Value>, intersect: Vec<Value>) -> Value {
    let mut result = Vec::new();
    for value in array {
        if intersect.contains(&value) {
            result.push(value);
        }
    }
    Value::Array(result)
}

#[derive(Clone)]
pub struct ArrayJoin;

impl Function for ArrayJoin {
    fn get_output_type(
        &self,
        argument_types: &[crate::pipeline::ValueType],
    ) -> Result<crate::pipeline::ValueType, crate::pipeline::PiperError> {
        match argument_types {
            [ValueType::Array, ValueType::String] => Ok(ValueType::String),
            [ValueType::Array, ValueType::String, ValueType::String] => Ok(ValueType::String),
            [ValueType::Dynamic, ValueType::String] => Ok(ValueType::String),
            [ValueType::Dynamic, _] => Ok(ValueType::String),
            [_, ValueType::Dynamic] => Ok(ValueType::String),
            [ValueType::Dynamic, _, _] => Ok(ValueType::String),
            [_, ValueType::Dynamic, _] => Ok(ValueType::String),
            [_, _, ValueType::Dynamic] => Ok(ValueType::String),
            _ => Err(crate::pipeline::PiperError::InvalidArgumentCount(
                2,
                argument_types.len(),
            )),
        }
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        match arguments.as_slice() {
            [Value::Array(array), Value::String(sep)] => {
                let mut strings = vec![];
                for value in array.iter().filter(|v| !v.is_null()) {
                    if let Ok(s) = value
                        .clone()
                        .convert_to(ValueType::String)
                        .get_string()
                        .map(|s| s.to_string())
                    {
                        strings.push(s);
                    };
                }
                strings.join(sep).into()
            }
            [Value::Array(array), Value::String(sep), Value::String(null_replace)] => {
                let mut strings = vec![];
                let null_replace: Value = null_replace.clone().into();
                for value in array {
                    let value = if value.is_null() {
                        &null_replace
                    } else {
                        value
                    };
                    if let Ok(s) = value
                        .clone()
                        .convert_to(ValueType::String)
                        .get_string()
                        .map(|s| s.to_string())
                    {
                        strings.push(s);
                    };
                }
                strings.join(sep).into()
            }
            _ => Value::Error(crate::pipeline::PiperError::InvalidArgumentCount(
                2,
                arguments.len(),
            )),
        }
    }
}

pub fn array_max(array: Vec<Value>) -> Value {
    let mut max = None;
    for value in array {
        if let Some(max) = max.as_mut() {
            if value > *max {
                *max = value;
            }
        } else {
            max = Some(value);
        }
    }
    max.unwrap_or(Value::Null)
}

pub fn array_min(array: Vec<Value>) -> Value {
    let mut min = None;
    for value in array {
        if let Some(min) = min.as_mut() {
            if value < *min {
                *min = value;
            }
        } else {
            min = Some(value);
        }
    }
    min.unwrap_or(Value::Null)
}

pub fn array_position(array: Vec<Value>, value: Value) -> Value {
    for (position, v) in array.into_iter().enumerate() {
        if v == value {
            // Spark SQL uses 1-based indexing
            return (position + 1).into();
        }
    }
    Value::Null
}

pub fn array_remove(array: Vec<Value>, value: Value) -> Value {
    let mut result = Vec::new();
    for v in array {
        if v != value {
            result.push(v);
        }
    }
    Value::Array(result)
}

pub fn array_repeat(value: Value, count: i64) -> Value {
    let mut result = Vec::new();
    for _ in 0..count {
        result.push(value.clone());
    }
    Value::Array(result)
}

pub fn array_size(array: Vec<Value>) -> Value {
    array.len().into()
}

pub fn array_union(array: Vec<Value>, union: Vec<Value>) -> Value {
    let mut result = array;
    for value in union {
        if !result.contains(&value) {
            result.push(value);
        }
    }
    Value::Array(result)
}

pub fn arrays_overlap(array: Vec<Value>, other: Vec<Value>) -> bool {
    for value in array {
        if !value.is_null() && other.contains(&value) {
            return true;
        }
    }
    false
}

pub fn arrays_zip(array: Vec<Value>, other: Vec<Value>) -> Value {
    let mut result = Vec::new();
    for (i, value) in array.into_iter().enumerate() {
        if let Some(other) = other.get(i) {
            result.push(Value::Array(vec![value, other.clone()]));
        } else {
            // The 2nd array is shorter than the 1st array
            break;
        }
    }
    Value::Array(result)
}

pub fn flatten(maybe_array: Value) -> Value {
    match maybe_array {
        Value::Array(array) => {
            let mut result = Vec::new();
            for item in array.into_iter() {
                match item {
                    Value::Array(array) => result.extend(array.into_iter()),
                    _ => result.push(item),
                }
            }
            result.into()
        }
        _ => maybe_array,
    }
}

#[cfg(test)]
mod tests {
    use crate::{Function, ValueType};

    #[test]
    fn test_array_distinct() {
        use super::array_distinct;
        use crate::pipeline::Value;
        assert_eq!(
            array_distinct(vec![1.into(), 2.into(), 3.into()]),
            vec![Value::Int(1), 2.into(), 3.into()].into(),
        );
        assert_eq!(
            array_distinct(vec![1.into(), 2.into(), 3.into(), 2.into()]),
            vec![Value::Int(1), 2.into(), 3.into()].into(),
        );
        assert_eq!(
            array_distinct(vec![1.into(), 2.into(), 3.into(), Value::Null]),
            vec![Value::Int(1), 2.into(), 3.into(), Value::Null].into(),
        );
    }

    #[test]
    fn test_array_contains() {
        use super::array_contains;
        use crate::pipeline::Value;
        assert_eq!(
            array_contains(vec![1.into(), 2.into(), 3.into()], 2.into()),
            true.into(),
        );
        assert_eq!(
            array_contains(vec![1.into(), 2.into(), 3.into()], 4.into()),
            false.into(),
        );
        assert_eq!(
            array_contains(vec![1.into(), 2.into(), 3.into()], Value::Null),
            false.into(),
        );
    }

    #[test]
    fn test_array_except() {
        use super::array_except;
        use crate::pipeline::Value;
        assert_eq!(
            array_except(vec![1.into(), 2.into(), 3.into()], vec![2.into(), 3.into()]),
            vec![Value::Int(1)].into(),
        );
        assert_eq!(
            array_except(vec![1.into(), 2.into(), 3.into()], vec![4.into(), 5.into()]),
            vec![Value::Int(1), 2.into(), 3.into()].into(),
        );
    }

    #[test]
    fn test_array_intersect() {
        use super::array_intersect;
        use crate::pipeline::Value;
        assert_eq!(
            array_intersect(vec![1.into(), 2.into(), 3.into()], vec![2.into(), 3.into()]),
            vec![Value::Int(2), 3.into()].into(),
        );
        assert_eq!(
            array_intersect(vec![1.into(), 2.into(), 3.into()], vec![4.into(), 5.into()]),
            Value::Array(vec![]),
        );
    }

    #[test]
    fn test_array_join() {
        use super::ArrayJoin;
        use crate::pipeline::Value;
        let array_join = ArrayJoin;
        assert!(array_join
            .get_output_type(&[ValueType::Array, ValueType::String])
            .is_ok());
        assert!(array_join
            .get_output_type(&[ValueType::Array, ValueType::String, ValueType::String])
            .is_ok());
        assert!(array_join
            .get_output_type(&[ValueType::Dynamic, ValueType::String])
            .is_ok());
        assert!(array_join
            .get_output_type(&[ValueType::Array, ValueType::Dynamic])
            .is_ok());
        assert!(array_join
            .get_output_type(&[ValueType::Dynamic, ValueType::Dynamic, ValueType::Dynamic])
            .is_ok());
        assert!(array_join
            .get_output_type(&[ValueType::Int, ValueType::Int, ValueType::Int])
            .is_err());
        assert_eq!(
            array_join.eval(vec![
                vec![Value::Int(1), 2.into(), 3.into()].into(),
                ",".into()
            ]),
            "1,2,3".into(),
        );
        assert_eq!(
            array_join.eval(vec![
                vec![Value::Int(1), 2.into(), Value::Null, 3.into()].into(),
                ",".into()
            ]),
            "1,2,3".into(),
        );
        assert_eq!(
            array_join.eval(vec![
                vec![Value::Int(1), 2.into(), Value::Null, 3.into()].into(),
                ",".into(),
                "x".into()
            ]),
            "1,2,x,3".into(),
        );
    }

    #[test]
    fn test_array_max() {
        use super::array_max;
        use crate::pipeline::Value;
        assert_eq!(array_max(vec![1.into(), 2.into(), 3.into()]), 3.into(),);
        assert_eq!(array_max(vec![1.into(), 2.into(), Value::Null]), 2.into(),);
        assert_eq!(array_max(vec![Value::Null, Value::Null]), Value::Null,);
    }

    #[test]
    fn test_array_min() {
        use super::array_min;
        use crate::pipeline::Value;
        assert_eq!(array_min(vec![1.into(), 2.into(), 3.into()]), 1.into(),);
        assert_eq!(array_min(vec![1.into(), 2.into(), Value::Null]), 1.into(),);
        assert_eq!(array_min(vec![Value::Null, Value::Null]), Value::Null,);
    }

    #[test]
    fn test_array_position() {
        use super::array_position;
        use crate::pipeline::Value;
        assert_eq!(
            array_position(vec![1.into(), 2.into(), 3.into()], 2.into()),
            2.into(),
        );
        assert_eq!(
            array_position(vec![1.into(), 2.into(), 3.into()], 4.into()),
            Value::Null,
        );
        assert_eq!(
            array_position(vec![1.into(), 2.into(), 3.into()], Value::Null),
            Value::Null,
        );
    }

    #[test]
    fn test_array_remove() {
        use super::array_remove;
        use crate::pipeline::Value;
        assert_eq!(
            array_remove(vec![1.into(), 2.into(), 3.into()], 2.into()),
            vec![Value::Int(1), 3.into()].into(),
        );
        assert_eq!(
            array_remove(vec![1.into(), 2.into(), 3.into()], 4.into()),
            vec![Value::Int(1), 2.into(), 3.into()].into(),
        );
    }

    #[test]
    fn test_array_repeat() {
        use super::array_repeat;
        use crate::pipeline::Value;
        assert_eq!(
            array_repeat(1.into(), 3.into()),
            vec![Value::Int(1), 1.into(), 1.into()].into(),
        );
        assert_eq!(array_repeat(1.into(), 0.into()), Value::Array(vec![]),);
    }

    #[test]
    fn test_array_size() {
        use super::array_size;
        assert_eq!(array_size(vec![1.into(), 2.into(), 3.into()]), 3.into(),);
        assert_eq!(array_size(vec![1.into(), 2.into()]), 2.into(),);
    }

    #[test]
    fn test_array_union() {
        use super::array_union;
        use crate::pipeline::Value;
        assert_eq!(
            array_union(vec![1.into(), 2.into(), 3.into()], vec![2.into(), 3.into()]),
            vec![Value::Int(1), 2.into(), 3.into()].into(),
        );
        assert_eq!(
            array_union(
                vec![1.into(), 2.into(), 3.into()],
                vec![3.into(), 4.into(), 5.into()]
            ),
            vec![Value::Int(1), 2.into(), 3.into(), 4.into(), 5.into()].into(),
        );
        assert_eq!(
            array_union(vec![1.into(), 2.into(), 3.into()], vec![4.into(), 5.into()]),
            vec![Value::Int(1), 2.into(), 3.into(), 4.into(), 5.into()].into(),
        );
    }

    #[test]
    fn test_array_overlap() {
        use super::arrays_overlap;
        use crate::pipeline::Value;
        assert!(arrays_overlap(
            vec![1.into(), 2.into(), 3.into()],
            vec![2.into(), 3.into()]
        ));
        assert!(arrays_overlap(
            vec![1.into(), 2.into(), 3.into()],
            vec![3.into(), 4.into(), 5.into()]
        ));
        assert!(!arrays_overlap(
            vec![1.into(), 2.into(), 3.into()],
            vec![4.into(), 5.into()]
        ),);
        assert!(!arrays_overlap(
            vec![1.into(), 2.into(), 3.into(), Value::Null],
            vec![4.into(), 5.into(), Value::Null]
        ));
    }

    #[test]
    fn test_arrays_zip() {
        use super::arrays_zip;
        use crate::pipeline::Value;
        assert_eq!(
            arrays_zip(vec![1.into(), 2.into(), 3.into()], vec![4.into(), 5.into()]),
            vec![
                Value::Array(vec![Value::Int(1), 4.into()]),
                Value::Array(vec![Value::Int(2), 5.into()]),
            ]
            .into(),
        );
        assert_eq!(
            arrays_zip(
                vec![1.into(), 2.into(), 3.into()],
                vec![4.into(), 5.into(), 6.into()]
            ),
            vec![
                Value::Array(vec![Value::Int(1), 4.into()]),
                Value::Array(vec![Value::Int(2), 5.into()]),
                Value::Array(vec![Value::Int(3), 6.into()]),
            ]
            .into(),
        );
        assert_eq!(
            arrays_zip(
                vec![1.into(), 2.into(), 3.into()],
                vec![4.into(), 5.into(), 6.into(), 7.into()]
            ),
            vec![
                Value::Array(vec![Value::Int(1), 4.into()]),
                Value::Array(vec![Value::Int(2), 5.into()]),
                Value::Array(vec![Value::Int(3), 6.into()]),
            ]
            .into(),
        );
    }

    #[test]
    fn test_flatten() {
        use super::flatten;
        use crate::pipeline::Value;
        assert_eq!(flatten(1.into()), 1.into(),);
        assert_eq!(
            flatten(Value::Array(vec![
                Value::Array(vec![1.into(), 2.into()]),
                Value::Array(vec![3.into(), 4.into()]),
            ])),
            Value::Array(vec![1.into(), 2.into(), 3.into(), 4.into()])
        );
        assert_eq!(
            flatten(Value::Array(vec![
                Value::Array(vec![1.into(), 2.into()]),
                Value::Array(vec![3.into(), 4.into()]),
                5.into(),
            ])),
            Value::Array(vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into()])
        );
        assert_eq!(
            flatten(Value::Array(vec![
                Value::Array(vec![1.into(), 2.into()]),
                Value::Array(vec![3.into(), 4.into()]),
                5.into(),
                Value::Array(vec![6.into(), 7.into()]),
            ])),
            Value::Array(vec![
                1.into(),
                2.into(),
                3.into(),
                4.into(),
                5.into(),
                6.into(),
                7.into()
            ])
        );
    }
}
