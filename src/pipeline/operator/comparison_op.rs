use crate::pipeline::{ValueType, PiperError, Value};

use super::Operator;

macro_rules! order_op {
    ($name:ident, $op:tt) => {
        #[derive(Clone, Debug)]
        pub struct $name;

        impl Operator for $name {
            fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
                if argument_types.len() != 2 {
                    return Err(PiperError::ArityError("+".to_string(), argument_types.len()));
                }
                if (argument_types[0].is_numeric() && argument_types[1].is_numeric())
                || (argument_types[0] == ValueType::String && argument_types[1] == ValueType::String) {
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
                    return Value::Error(PiperError::ArityError("+".to_string(), arguments.len()));
                }

                match arguments.as_slice() {
                    [Value::Int(a), Value::Int(b)] => (a $op b).into(),
                    [Value::Int(a), Value::Long(b)] => ((a.clone() as i64) $op (b.clone())).into(),
                    [Value::Int(a), Value::Float(b)] => ((a.clone() as f64) $op (b.clone() as f64)).into(),
                    [Value::Int(a), Value::Double(b)] => ((a.clone() as f64) $op (b.clone())).into(),

                    [Value::Long(a), Value::Int(b)] => (a.clone() $op b.clone() as i64).into(),
                    [Value::Long(a), Value::Long(b)] => (a $op b).into(),
                    [Value::Long(a), Value::Float(b)] => ((a.clone() as f64) $op (b.clone() as f64)).into(),
                    [Value::Long(a), Value::Double(b)] => ((a.clone() as f64) $op (b.clone())).into(),

                    [Value::Float(a), Value::Int(b)] => ((a.clone() as f64) $op (b.clone() as f64)).into(),
                    [Value::Float(a), Value::Long(b)] => ((a.clone() as f64) $op (b.clone() as f64)).into(),
                    [Value::Float(a), Value::Float(b)] => (a $op b).into(),
                    [Value::Float(a), Value::Double(b)] => ((a.clone() as f64) $op (b.clone() as f64)).into(),

                    [Value::Double(a), Value::Int(b)] => (a.clone() $op b.clone() as f64).into(),
                    [Value::Double(a), Value::Long(b)] => (a.clone() $op b.clone() as f64).into(),
                    [Value::Double(a), Value::Float(b)] => (a.clone() $op b.clone() as f64).into(),
                    [Value::Double(a), Value::Double(b)] => (a.clone() $op b.clone() as f64).into(),

                    [Value::Bool(a), Value::Bool(b)] => (a $op b).into(),
                    [Value::String(a), Value::String(b)] => (a $op b).into(),
                    
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
                format!("({} {} {})", arguments[0], stringify!($op), arguments[1])
            }
        }
    };
}

order_op!(LessThanOperator, <);
order_op!(GreaterThanOperator, >);
order_op!(LessEqualOperator, <=);
order_op!(GreaterEqualOperator, >=);

macro_rules! compare_op {
    ($name:ident, $op:tt) => {
        #[derive(Clone, Debug)]
        pub struct $name;

        impl Operator for $name {
            fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
                if argument_types.len() != 2 {
                    return Err(PiperError::ArityError("+".to_string(), argument_types.len()));
                }
                if (argument_types[0] == ValueType::Dynamic || argument_types[1] == ValueType::Dynamic) {
                    Ok(ValueType::Dynamic)
                } else if (argument_types[0].is_numeric() && argument_types[1].is_numeric())
                || (argument_types[0] == ValueType::String && argument_types[1] == ValueType::String) {
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
                    return Value::Error(PiperError::ArityError("+".to_string(), arguments.len()));
                }

                match arguments.as_slice() {
                    [Value::Int(a), Value::Int(b)] => (a $op b).into(),
                    [Value::Int(a), Value::Long(b)] => ((a.clone() as i64) $op (b.clone())).into(),
                    [Value::Int(a), Value::Float(b)] => ((a.clone() as f64) $op (b.clone() as f64)).into(),
                    [Value::Int(a), Value::Double(b)] => ((a.clone() as f64) $op (b.clone())).into(),

                    [Value::Long(a), Value::Int(b)] => (a.clone() $op b.clone() as i64).into(),
                    [Value::Long(a), Value::Long(b)] => (a $op b).into(),
                    [Value::Long(a), Value::Float(b)] => ((a.clone() as f64) $op (b.clone() as f64)).into(),
                    [Value::Long(a), Value::Double(b)] => ((a.clone() as f64) $op (b.clone())).into(),

                    [Value::Float(a), Value::Int(b)] => ((a.clone() as f64) $op (b.clone() as f64)).into(),
                    [Value::Float(a), Value::Long(b)] => ((a.clone() as f64) $op (b.clone() as f64)).into(),
                    [Value::Float(a), Value::Float(b)] => (a $op b).into(),
                    [Value::Float(a), Value::Double(b)] => ((a.clone() as f64) $op (b.clone() as f64)).into(),

                    [Value::Double(a), Value::Int(b)] => (a.clone() $op b.clone() as f64).into(),
                    [Value::Double(a), Value::Long(b)] => (a.clone() $op b.clone() as f64).into(),
                    [Value::Double(a), Value::Float(b)] => (a.clone() $op b.clone() as f64).into(),
                    [Value::Double(a), Value::Double(b)] => (a.clone() $op b.clone() as f64).into(),

                    [Value::Bool(a), Value::Bool(b)] => (a $op b).into(),
                    [Value::String(a), Value::String(b)] => (a $op b).into(),
                    [Value::Array(a), Value::Array(b)] => (a $op b).into(),
                    [Value::Object(a), Value::Object(b)] => (a $op b).into(),
                    
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
                format!("({} {} {})", arguments[0], stringify!($op), arguments[1])
            }
        }
    };
}

compare_op!(EqualOperator, ==);
compare_op!(NotEqualOperator, !=);
