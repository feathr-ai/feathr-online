use crate::pipeline::{PiperError, Value, ValueType};

use super::Operator;

#[derive(Clone, Debug)]
pub struct ArrayIndexOperator;

impl Operator for ArrayIndexOperator {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 2 {
            return Err(PiperError::ArityError(
                "[]".to_string(),
                argument_types.len(),
            ));
        }
        if argument_types[0] != ValueType::Array
            && argument_types[0] != ValueType::Object
            && argument_types[0] != ValueType::Dynamic
        {
            return Err(PiperError::InvalidArgumentType(
                "[]".to_string(),
                0,
                argument_types[0],
            ));
        }
        Ok(ValueType::Dynamic)
    }

    fn eval(&self, mut arguments: Vec<Value>) -> Value {
        if arguments.len() != 2 {
            return Value::Error(PiperError::ArityError("[]".to_string(), arguments.len()));
        }

        let b = arguments.remove(1);
        let a = arguments.remove(0);
        match [a, b] {
            [Value::Array(mut a), Value::Int(b)] => a.remove(b as usize),
            [Value::Array(mut a), Value::Long(b)] => a.remove(b as usize),
            [Value::Object(mut a), Value::String(b)] => a.remove(b.as_ref()).unwrap_or_default(),

            // All other combinations are invalid
            _ => Value::Error(PiperError::TypeMismatch(
                "[]".to_string(),
                arguments[0].value_type(),
                arguments[1].value_type(),
            )),
        }
    }

    fn dump(&self, arguments: Vec<String>) -> String {
        format!("({}[{}])", arguments[0], arguments[1])
    }
}

#[derive(Clone, Debug)]
pub struct MapIndexOperator;

impl Operator for MapIndexOperator {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 2 {
            return Err(PiperError::ArityError(
                ".".to_string(),
                argument_types.len(),
            ));
        }
        if argument_types[0] != ValueType::Object && argument_types[0] != ValueType::Dynamic {
            return Err(PiperError::InvalidArgumentType(
                ".".to_string(),
                0,
                argument_types[0],
            ));
        }
        if argument_types[1] != ValueType::String {
            return Err(PiperError::InvalidArgumentType(
                ".".to_string(),
                0,
                argument_types[1],
            ));
        }
        Ok(ValueType::Dynamic)
    }

    fn eval(&self, mut arguments: Vec<Value>) -> Value {
        if arguments.len() != 2 {
            return Value::Error(PiperError::ArityError(".".to_string(), arguments.len()));
        }

        let b = arguments.remove(1);
        let a = arguments.remove(0);
        match [a, b] {
            [Value::Object(mut a), Value::String(b)] => a.remove(b.as_ref()).unwrap_or(Value::Null),

            // All other combinations are invalid
            _ => Value::Error(PiperError::TypeMismatch(
                ".".to_string(),
                arguments[0].value_type(),
                arguments[1].value_type(),
            )),
        }
    }

    fn dump(&self, arguments: Vec<String>) -> String {
        format!("{}.{})", arguments[0], arguments[1])
    }
}

#[cfg(test)]
mod tests {
    use crate::ValueType;

    #[test]
    fn test_array_index() {
        use crate::pipeline::operator::index_op::ArrayIndexOperator;
        use crate::pipeline::operator::Operator;
        use crate::pipeline::Value;

        let op = ArrayIndexOperator;
        assert!(op
            .get_output_type(&[ValueType::Array, ValueType::Int])
            .is_ok());
        assert!(op
            .get_output_type(&[ValueType::Int, ValueType::Int])
            .is_err());
        assert_eq!(
            op.eval(vec![
                Value::Array(vec![Value::Int(1), Value::Int(2)]),
                Value::Int(0)
            ]),
            Value::Int(1)
        );
        assert_eq!(
            op.eval(vec![
                Value::Array(vec![Value::Int(1), Value::Int(2)]),
                Value::Int(1)
            ]),
            Value::Int(2)
        );
    }

    #[test]
    fn test_map_index() {
        use crate::pipeline::operator::index_op::MapIndexOperator;
        use crate::pipeline::operator::Operator;
        use crate::pipeline::Value;

        let op = MapIndexOperator;
        assert!(op
            .get_output_type(&[ValueType::Object, ValueType::String])
            .is_ok());
        assert!(op
            .get_output_type(&[ValueType::Object, ValueType::Int])
            .is_err());
        assert_eq!(
            op.eval(vec![
                Value::Object(
                    vec![("a".into(), Value::Int(1)), ("b".into(), Value::Int(2)),]
                        .into_iter()
                        .collect()
                ),
                Value::String("a".into())
            ]),
            Value::Int(1)
        );
        assert_eq!(
            op.eval(vec![
                Value::Object(
                    vec![("a".into(), Value::Int(1)), ("b".into(), Value::Int(2)),]
                        .into_iter()
                        .collect()
                ),
                Value::String("b".into())
            ]),
            Value::Int(2)
        );
    }
}
