use crate::pipeline::{PiperError, Value, ValueType};

use super::Operator;

#[derive(Clone, Debug)]
pub struct PositiveOperator;

impl Operator for PositiveOperator {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 1 {
            return Err(PiperError::ArityError(
                "+".to_string(),
                argument_types.len(),
            ));
        }
        match argument_types {
            [ValueType::Dynamic] => Ok(ValueType::Dynamic),
            [ValueType::Int] => Ok(ValueType::Int),
            [ValueType::Long] => Ok(ValueType::Long),
            [ValueType::Float] => Ok(ValueType::Float),
            [ValueType::Double] => Ok(ValueType::Double),
            [a] => Err(PiperError::InvalidOperandType(
                stringify!($op).to_string(),
                *a,
            ))?,
            _ => unreachable!("Unknown error."),
        }
    }
    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() != 1 {
            return Value::Error(PiperError::ArityError("+".to_string(), arguments.len()));
        }

        match arguments.as_slice() {
            [Value::Int(a)] => (*a).into(),
            [Value::Long(a)] => (*a).into(),
            [Value::Float(a)] => (*a).into(),
            [Value::Double(a)] => (*a).into(),

            // All other combinations are invalid
            [a] => Value::Error(PiperError::InvalidOperandType(
                "+".to_string(),
                a.value_type(),
            )),

            // Shouldn't reach here
            _ => unreachable!("Unknown error."),
        }
    }

    fn dump(&self, arguments: Vec<String>) -> String {
        format!("(+ {})", arguments[0])
    }
}

#[derive(Clone, Debug)]
pub struct NegativeOperator;

impl Operator for NegativeOperator {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 1 {
            return Err(PiperError::ArityError(
                "+".to_string(),
                argument_types.len(),
            ));
        }
        match argument_types {
            [ValueType::Dynamic] => Ok(ValueType::Dynamic),
            [ValueType::Int] => Ok(ValueType::Int),
            [ValueType::Long] => Ok(ValueType::Long),
            [ValueType::Float] => Ok(ValueType::Float),
            [ValueType::Double] => Ok(ValueType::Double),
            [a] => Err(PiperError::InvalidOperandType(
                stringify!($op).to_string(),
                *a,
            ))?,
            _ => unreachable!("Unknown error."),
        }
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() != 1 {
            return Value::Error(PiperError::ArityError("-".to_string(), arguments.len()));
        }

        match arguments.as_slice() {
            [Value::Int(a)] => (-*a).into(),
            [Value::Long(a)] => (-*a).into(),
            [Value::Float(a)] => (-*a).into(),
            [Value::Double(a)] => (-*a).into(),

            [a] => Value::Error(PiperError::InvalidOperandType(
                "-".to_string(),
                a.value_type(),
            )),

            // Shouldn't reach here
            _ => unreachable!("Unknown error."),
        }
    }

    fn dump(&self, arguments: Vec<String>) -> String {
        format!("(- {})", arguments[0])
    }
}

#[derive(Clone, Debug)]
pub struct NotOperator;

impl Operator for NotOperator {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 1 {
            return Err(PiperError::ArityError(
                "+".to_string(),
                argument_types.len(),
            ));
        }
        match argument_types {
            [ValueType::Dynamic] => Ok(ValueType::Bool),
            [ValueType::Bool] => Ok(ValueType::Bool),

            [a] => Err(PiperError::InvalidOperandType(
                stringify!($op).to_string(),
                *a,
            ))?,
            _ => unreachable!("Unknown error."),
        }
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() != 1 {
            return Value::Error(PiperError::ArityError("not".to_string(), arguments.len()));
        }

        match arguments.as_slice() {
            [Value::Bool(a)] => (!*a).into(),
            [a] => Value::Error(PiperError::InvalidOperandType(
                "not".to_string(),
                a.value_type(),
            )),

            // Shouldn't reach here
            _ => unreachable!("Unknown error."),
        }
    }

    fn dump(&self, arguments: Vec<String>) -> String {
        format!("(not {})", arguments[0])
    }
}

#[derive(Clone, Debug)]
pub struct IsNullOperator;

impl Operator for IsNullOperator {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 1 {
            return Err(PiperError::ArityError(
                "is null".to_string(),
                argument_types.len(),
            ));
        }
        Ok(ValueType::Bool)
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() != 1 {
            return Value::Error(PiperError::ArityError(
                "is null".to_string(),
                arguments.len(),
            ));
        }

        match arguments.as_slice() {
            [Value::Null] => true.into(),
            [_] => false.into(),

            // Shouldn't reach here
            _ => unreachable!("Unknown error."),
        }
    }

    fn dump(&self, arguments: Vec<String>) -> String {
        format!("({} is null)", arguments[0])
    }
}

#[derive(Clone, Debug)]
pub struct IsNotNullOperator;

impl Operator for IsNotNullOperator {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 1 {
            return Err(PiperError::ArityError(
                "is not null".to_string(),
                argument_types.len(),
            ));
        }
        Ok(ValueType::Bool)
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() != 1 {
            return Value::Error(PiperError::ArityError(
                "is not null".to_string(),
                arguments.len(),
            ));
        }

        match arguments.as_slice() {
            [Value::Null] => false.into(),
            [_] => true.into(),

            // Shouldn't reach here
            _ => unreachable!("Unknown error."),
        }
    }

    fn dump(&self, arguments: Vec<String>) -> String {
        format!("({} is not null)", arguments[0])
    }
}
