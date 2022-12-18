use std::marker::PhantomData;

use crate::pipeline::{PiperError, Value, ValueType, ValueTypeOf};

use super::Function;

#[derive(Clone)]
struct BinaryFunctionWrapper<A1, A2, R, F, E1, E2>
where
    A1: Send + Sync + TryFrom<Value, Error = E1>,
    A2: Send + Sync + TryFrom<Value, Error = E2>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    E1: Sync + Send,
    E2: Sync + Send,
    F: Fn(A1, A2) -> R + Clone,
{
    function: F,
    _phantom: PhantomData<(A1, A2, R, E1, E2)>,
}

impl<A1, A2, R, F, E1, E2> Function for BinaryFunctionWrapper<A1, A2, R, F, E1, E2>
where
    A1: Send + Sync + Clone + TryFrom<Value, Error = E1>,
    A2: Send + Sync + Clone + TryFrom<Value, Error = E2>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    F: Fn(A1, A2) -> R + Sync + Send + Clone,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    E1: Sync + Send + Clone,
    E2: Sync + Send + Clone,
{
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() > 2 {
            return Err(PiperError::InvalidArgumentCount(2, argument_types.len()));
        }
        Ok(R::value_type())
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() > 2 {
            return Value::Error(PiperError::InvalidArgumentCount(2, arguments.len()));
        }

        let mut args = arguments.into_iter();

        let a1: Result<A1, E1> = args.next().unwrap_or_default().try_into();
        let a2: Result<A2, E2> = args.next().unwrap_or_default().try_into();

        match (a1, a2) {
            (Ok(a1), Ok(a2)) => (self.function)(a1, a2).into(),
            (Err(e), _) => Err(e).into(),
            (_, Err(e)) => Err(e).into(),
        }
    }
}

pub fn binary_fn<A1, A2, R, F, E1, E2>(f: F) -> Box<impl Function>
where
    A1: Send + Sync + Clone + TryFrom<Value, Error = E1>,
    A2: Send + Sync + Clone + TryFrom<Value, Error = E2>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    E1: Sync + Send + Clone,
    E2: Sync + Send + Clone,
    F: Fn(A1, A2) -> R + Sync + Send + Clone,
{
    Box::new(BinaryFunctionWrapper {
        function: f,
        _phantom: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use crate::{Function, ValueType};

    #[test]
    fn test_bin() {
        let f = super::binary_fn(|a: i32, b: i32| a + b);
        assert_eq!(f.eval(vec![1.into(), 2.into()]), 3.into());
        assert!(f.get_output_type(&[ValueType::Int]).is_ok());
        assert!(f.get_output_type(&[ValueType::Int, ValueType::Int]).is_ok());
        assert!(f
            .get_output_type(&[ValueType::Int, ValueType::Int, ValueType::Int])
            .is_err());
    }
}
