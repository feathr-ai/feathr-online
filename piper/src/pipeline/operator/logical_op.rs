use crate::pipeline::{PiperError, Value, ValueType};

use super::Operator;

macro_rules! logical_op {
    ($name:ident, $op_name:tt, $op:tt) => {
        #[derive(Clone, Debug)]
        pub struct $name;

        impl Operator for $name {
            fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
                if argument_types.len() != 2 {
                    return Err(PiperError::ArityError("and".to_string(), argument_types.len()));
                }
                if (argument_types[0] == ValueType::Dynamic || argument_types[1] == ValueType::Dynamic) {
                    Ok(ValueType::Bool)
                } else if (argument_types[0] == ValueType::Bool && argument_types[1] == ValueType::Bool) {
                    Ok(ValueType::Bool)
                } else {
                    Err(PiperError::TypeMismatch(
                        stringify!($op).to_string(),
                        argument_types[0],
                        argument_types[1],
                    ))
                }
            }

            fn eval(&self, arguments: Vec<Value>) -> Value {
                if arguments.len() != 2 {
                    return Value::Error(PiperError::ArityError("and".to_string(), arguments.len()));
                }

                match arguments.as_slice() {
                    [Value::Bool(a), Value::Bool(b)] => (*a $op *b).into(),

                    // All other combinations are invalid
                    [a, b] => Value::Error(PiperError::TypeMismatch(
                        stringify!($op).to_string(),
                        a.value_type(),
                        b.value_type(),
                    )),

                    // Shouldn't reach here
                    _ => unreachable!("Unknown error."),
                }
            }

            fn dump(&self, arguments: Vec<String>) -> String {
                format!("({} {} {})", arguments[0], stringify!($op_name), arguments[1])
            }
        }
    };
}

logical_op!(AndOperator, and, &&);
logical_op!(OrOperator, or, ||);
