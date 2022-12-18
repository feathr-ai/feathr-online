use std::marker::PhantomData;

use crate::pipeline::{PiperError, Value, ValueType, ValueTypeOf};

use super::Function;

#[derive(Clone)]
struct NullaryFunctionWrapper<R, F>
where
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    F: Fn() -> R + Clone,
{
    function: F,
    output_type: PhantomData<R>,
}

impl<R, F> Function for NullaryFunctionWrapper<R, F>
where
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    F: Fn() -> R + Sync + Send + Clone,
{
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if !argument_types.is_empty() {
            return Err(PiperError::InvalidArgumentCount(0, argument_types.len()));
        }
        Ok(R::value_type())
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        match arguments.as_slice() {
            [] => (self.function)().into(),
            _ => Value::Error(PiperError::InvalidArgumentCount(0, arguments.len())),
        }
    }
}

pub fn nullary_fn<R, F>(f: F) -> Box<impl Function>
where
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    F: Fn() -> R + Sync + Send + Clone,
{
    Box::new(NullaryFunctionWrapper {
        function: f,
        output_type: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use crate::{ValueType, Function};

    #[test]
    fn test_nullary() {
        let f = super::nullary_fn(|| 42i32);
        assert_eq!(f.eval(vec![]), 42.into());
        assert!(f.get_output_type(&[]).is_ok());
        assert!(f.get_output_type(&[ValueType::Int]).is_err());
    }
}