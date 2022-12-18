use std::marker::PhantomData;

use crate::pipeline::{PiperError, Value, ValueType, ValueTypeOf};

use super::Function;

#[derive(Clone)]
struct TernaryFunctionWrapper<A1, A2, A3, R, F, E1, E2, E3>
where
    A1: Send + Sync + Clone + TryFrom<Value, Error = E1>,
    A2: Send + Sync + Clone + TryFrom<Value, Error = E2>,
    A3: Send + Sync + Clone + TryFrom<Value, Error = E3>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    Result<Value, E3>: Into<Value>,
    E1: Sync + Send + Clone,
    E2: Sync + Send + Clone,
    E3: Sync + Send + Clone,
    F: Fn(A1, A2, A3) -> R + Clone,
{
    function: F,
    _phantom: PhantomData<(A1, A2, A3, R, E1, E2, E3)>,
}

impl<A1, A2, A3, R, F, E1, E2, E3> Function for TernaryFunctionWrapper<A1, A2, A3, R, F, E1, E2, E3>
where
    A1: Send + Sync + Clone + TryFrom<Value, Error = E1>,
    A2: Send + Sync + Clone + TryFrom<Value, Error = E2>,
    A3: Send + Sync + Clone + TryFrom<Value, Error = E3>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    F: Fn(A1, A2, A3) -> R + Sync + Send + Clone,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    Result<Value, E3>: Into<Value>,
    E1: Sync + Send + Clone,
    E2: Sync + Send + Clone,
    E3: Sync + Send + Clone,
{
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() > 3 {
            return Err(PiperError::InvalidArgumentCount(3, argument_types.len()));
        }
        Ok(R::value_type())
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() > 3 {
            return Value::Error(PiperError::InvalidArgumentCount(3, arguments.len()));
        }

        let mut args = arguments.into_iter();

        let a1: Result<A1, E1> = args.next().unwrap_or_default().try_into();
        let a2: Result<A2, E2> = args.next().unwrap_or_default().try_into();
        let a3: Result<A3, E3> = args.next().unwrap_or_default().try_into();

        match (a1, a2, a3) {
            (Ok(a1), Ok(a2), Ok(a3)) => (self.function)(a1, a2, a3).into(),
            (Err(e), _, _) => Err(e).into(),
            (_, Err(e), _) => Err(e).into(),
            (_, _, Err(e)) => Err(e).into(),
        }
    }
}

pub fn ternary_fn<A1, A2, A3, R, F, E1, E2, E3>(f: F) -> Box<impl Function>
where
    A1: Send + Sync + Clone + TryFrom<Value, Error = E1>,
    A2: Send + Sync + Clone + TryFrom<Value, Error = E2>,
    A3: Send + Sync + Clone + TryFrom<Value, Error = E3>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    Result<Value, E3>: Into<Value>,
    E1: Sync + Send + Clone,
    E2: Sync + Send + Clone,
    E3: Sync + Send + Clone,
    F: Fn(A1, A2, A3) -> R + Sync + Send + Clone,
{
    Box::new(TernaryFunctionWrapper {
        function: f,
        _phantom: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use crate::{ValueType, Function};

    #[test]
    fn test_ter() {
        let f = super::ternary_fn(|a: i32, b: i32, c: Option<i32>| a + b - c.unwrap_or_default());
        assert_eq!(f.eval(vec![1.into(), 5.into(), 2.into()]), 4.into());
        // Optional argument
        assert_eq!(f.eval(vec![1.into(), 5.into()]), 6.into());
        assert!(f.get_output_type(&[ValueType::Int]).is_ok());
        assert!(f.get_output_type(&[ValueType::Int, ValueType::Int]).is_ok());
        assert!(f.get_output_type(&[ValueType::Int, ValueType::Int, ValueType::Int]).is_ok());
        assert!(f.get_output_type(&[ValueType::Int, ValueType::Int, ValueType::Int, ValueType::Int]).is_err());
    }
}