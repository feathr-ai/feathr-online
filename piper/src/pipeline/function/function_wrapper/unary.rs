use std::marker::PhantomData;

use crate::pipeline::{PiperError, Value, ValueType, ValueTypeOf};

use super::Function;

#[derive(Clone)]
struct UnaryFunctionWrapper<A, R, F, E>
where
    A: Send + Sync + Clone + TryFrom<Value, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    Result<Value, E>: Into<Value>,
    E: Sync + Send + Clone,
    F: Fn(A) -> R + Clone,
{
    function: F,
    _phantom: PhantomData<(A, R, E)>,
}

impl<A, R, F, E> Function for UnaryFunctionWrapper<A, R, F, E>
where
    A: Send + Sync + Clone + TryFrom<Value, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    F: (Fn(A) -> R) + Sync + Send + Clone,
    Result<Value, E>: Into<Value>,
    E: Sync + Send + Clone,
{
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() > 1 {
            return Err(PiperError::InvalidArgumentCount(1, argument_types.len()));
        }
        Ok(R::value_type())
    }

    fn eval(&self, mut arguments: Vec<Value>) -> Value {
        if arguments.len() > 1 {
            return Value::Error(PiperError::InvalidArgumentCount(1, arguments.len()));
        }

        match arguments.pop().unwrap_or_default().try_into() {
            Ok(a) => (self.function)(a).into(),
            Err(e) => Err(e).into(),
        }
    }
}

pub fn unary_fn<A, R, F, E>(f: F) -> Box<impl Function>
where
    A: Send + Sync + Clone + TryFrom<Value, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    F: Fn(A) -> R + Sync + Send + Clone,
    Result<Value, E>: Into<Value>,
    E: Sync + Send + Clone,
{
    Box::new(UnaryFunctionWrapper {
        function: f,
        _phantom: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use crate::{ValueType, Function};

    #[test]
    fn test_uni() {
        let f = super::unary_fn(|a: i32| a + 42);
        assert_eq!(f.eval(vec![1.into()]), 43.into());
        assert!(f.get_output_type(&[]).is_ok());
        assert!(f.get_output_type(&[ValueType::Int]).is_ok());
        assert!(f.get_output_type(&[ValueType::Int, ValueType::Int]).is_err());
    }
}