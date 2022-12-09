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

impl<A1, A2, A3, R, F, E1, E2, E3> TernaryFunctionWrapper<A1, A2, A3, R, F, E1, E2, E3>
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
    fn new(function: F) -> Self {
        Self {
            function,
            _phantom: PhantomData,
        }
    }

    fn invoke(&self, args: &[Value]) -> Value {
        if args.len() > 3 {
            return Value::Error(PiperError::InvalidArgumentCount(3, args.len()));
        }

        let a1: Result<A1, E1> = args.get(0).cloned().unwrap_or_default().try_into();
        let a2: Result<A2, E2> = args.get(1).cloned().unwrap_or_default().try_into();
        let a3: Result<A3, E3> = args.get(2).cloned().unwrap_or_default().try_into();

        match (a1, a2, a3) {
            (Ok(a1), Ok(a2), Ok(a3)) => (self.function)(a1, a2, a3).into(),
            (Err(e), _, _) => Err(e).into(),
            (_, Err(e), _) => Err(e).into(),
            (_, _, Err(e)) => Err(e).into(),
        }
    }
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
    fn get_output_type(&self, _argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        Ok(R::value_type())
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        self.invoke(&arguments)
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
    Box::new(TernaryFunctionWrapper::new(f))
}
