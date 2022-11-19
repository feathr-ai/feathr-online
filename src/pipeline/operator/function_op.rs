use crate::pipeline::{PiperError, Value, ValueType};

use super::super::function::Function;
use super::Operator;

#[derive(Clone, Debug)]
pub struct FunctionOperator {
    pub name: &'static str,
    pub function: &'static Box<dyn Function>,
}

impl Operator for FunctionOperator {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        self.function.get_output_type(argument_types)
    }

    fn eval(&self, arguments: Vec<Value>) -> Result<Value, PiperError> {
        self.function.eval(arguments)
    }

    fn dump(&self, arguments: Vec<String>) -> String {
        format!("{}({})", self.name, arguments.join(", "))
    }
}
