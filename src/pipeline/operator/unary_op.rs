use crate::pipeline::{ValueType, PiperError, Value};

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
    fn eval(&self, arguments: Vec<Value>) -> Result<Value, PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::ArityError("+".to_string(), arguments.len()));
        }

        Ok(match arguments.as_slice() {
            [Value::Int(a)] => (a.clone()).into(),
            [Value::Long(a)] => (a.clone()).into(),
            [Value::Float(a)] => (a.clone()).into(),
            [Value::Double(a)] => (a.clone()).into(),

            // All other combinations are invalid
            [a] => Err(PiperError::InvalidOperandType(
                "+".to_string(),
                a.value_type(),
            ))?,

            // Shouldn't reach here
            _ => unreachable!("Unknown error."),
        })
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

    fn eval(&self, arguments: Vec<Value>) -> Result<Value, PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::ArityError("-".to_string(), arguments.len()));
        }

        Ok(match arguments.as_slice() {
            [Value::Int(a)] => (-a.clone()).into(),
            [Value::Long(a)] => (-a.clone()).into(),
            [Value::Float(a)] => (-a.clone()).into(),
            [Value::Double(a)] => (-a.clone()).into(),

            [a] => Err(PiperError::InvalidOperandType(
                "-".to_string(),
                a.value_type(),
            ))?,

            // Shouldn't reach here
            _ => unreachable!("Unknown error."),
        })
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
            [ValueType::Bool] => Ok(ValueType::Bool),

            [a] => Err(PiperError::InvalidOperandType(
                stringify!($op).to_string(),
                *a,
            ))?,
            _ => unreachable!("Unknown error."),
        }
    }

    fn eval(&self, arguments: Vec<Value>) -> Result<Value, PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::ArityError("not".to_string(), arguments.len()));
        }

        Ok(match arguments.as_slice() {
            [Value::Bool(a)] => (!a.clone()).into(),
            [a] => Err(PiperError::InvalidOperandType(
                "not".to_string(),
                a.value_type(),
            ))?,

            // Shouldn't reach here
            _ => unreachable!("Unknown error."),
        })
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

    fn eval(&self, arguments: Vec<Value>) -> Result<Value, PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::ArityError(
                "is null".to_string(),
                arguments.len(),
            ));
        }

        Ok(match arguments.as_slice() {
            [Value::Null] => true.into(),
            [_] => false.into(),

            // Shouldn't reach here
            _ => unreachable!("Unknown error."),
        })
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

    fn eval(&self, arguments: Vec<Value>) -> Result<Value, PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::ArityError(
                "is not null".to_string(),
                arguments.len(),
            ));
        }

        Ok(match arguments.as_slice() {
            [Value::Null] => false.into(),
            [_] => true.into(),

            // Shouldn't reach here
            _ => unreachable!("Unknown error."),
        })
    }

    fn dump(&self, arguments: Vec<String>) -> String {
        format!("({} is not null)", arguments[0])
    }
}
