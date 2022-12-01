use crate::common::IgnoreDebug;
use crate::pipeline::{PiperError, Value, ValueType};

use super::super::function::Function;
use super::Operator;

#[derive(Clone, Debug)]
pub struct FunctionOperator {
    pub name: String,
    pub function: IgnoreDebug<Box<dyn Function>>,
}

impl Operator for FunctionOperator {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        self.function.inner.get_output_type(argument_types)
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        self.function.inner.eval(arguments)
    }

    fn dump(&self, arguments: Vec<String>) -> String {
        format!("{}({})", self.name, arguments.join(", "))
    }
}
