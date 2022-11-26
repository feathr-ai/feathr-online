use std::marker::PhantomData;

use crate::pipeline::{PiperError, Value, ValueType, ValueTypeOf};

use super::Function;

struct NullaryFunctionWrapper<R, F>
where
    R: Into<Value> + Sync + Send + ValueTypeOf,
    F: Fn() -> R,
{
    function: F,
    output_type: PhantomData<R>,
}

impl<R, F> NullaryFunctionWrapper<R, F>
where
    R: Into<Value> + Sync + Send + ValueTypeOf,
    F: Fn() -> R,
{
    fn new(function: F) -> Self {
        Self {
            function,
            output_type: PhantomData,
        }
    }

    fn invoke(&self) -> Value {
        (self.function)().into()
    }
}

impl<R, F> Function for NullaryFunctionWrapper<R, F>
where
    R: Into<Value> + Sync + Send + ValueTypeOf,
    F: Fn() -> R + Sync + Send,
{
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if !argument_types.is_empty() {
            return Err(PiperError::InvalidArgumentCount(0, argument_types.len()));
        }
        Ok(R::value_type())
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        match arguments.as_slice() {
            [] => self.invoke(),
            _ => Value::Error(PiperError::InvalidArgumentCount(0, arguments.len())),
        }
    }
}

pub fn nullary_fn<R, F>(f: F) -> Box<impl Function>
where
    R: Into<Value> + Sync + Send + ValueTypeOf,
    F: Fn() -> R + Sync + Send,
{
    Box::new(NullaryFunctionWrapper::new(f))
}
