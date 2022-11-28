use std::marker::PhantomData;

use crate::pipeline::{PiperError, Value, ValueType, ValueTypeOf};

use super::Function;

struct BinaryFunctionWrapper<A1, A2, R, F, E1, E2>
where
    A1: Send + Sync,
    A2: Send + Sync,
    Value: TryInto<A1, Error = E1>,
    Value: TryInto<A2, Error = E2>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    E1: Sync + Send,
    E2: Sync + Send,
    F: Fn(A1, A2) -> R,
{
    function: F,
    _phantom: PhantomData<(A1, A2, R, E1, E2)>,
}

impl<A1, A2, R, F, E1, E2> BinaryFunctionWrapper<A1, A2, R, F, E1, E2>
where
    A1: Send + Sync,
    A2: Send + Sync,
    Value: TryInto<A1, Error = E1>,
    Value: TryInto<A2, Error = E2>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    E1: Sync + Send,
    E2: Sync + Send,
    F: Fn(A1, A2) -> R,
{
    fn new(function: F) -> Self {
        Self {
            function,
            _phantom: PhantomData,
        }
    }

    fn invoke(&self, args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Error(PiperError::InvalidArgumentCount(2, args.len()));
        }

        let a1: Result<A1, E1> = args[0].clone().try_into();
        let a2: Result<A2, E2> = args[1].clone().try_into();

        match (a1, a2) {
            (Ok(a1), Ok(a2)) => (self.function)(a1, a2).into(),
            (Err(e), _) => Err(e).into(),
            (_, Err(e)) => Err(e).into(),
        }
    }
}

impl<A1, A2, R, F, E1, E2> Function for BinaryFunctionWrapper<A1, A2, R, F, E1, E2>
where
    A1: Send + Sync,
    A2: Send + Sync,
    Value: TryInto<A1, Error = E1>,
    Value: TryInto<A2, Error = E2>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    F: Fn(A1, A2) -> R + Sync + Send,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    E1: Sync + Send,
    E2: Sync + Send,
{
    fn get_output_type(&self, _argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        Ok(R::value_type())
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        self.invoke(&arguments)
    }
}

pub fn binary_fn<A1, A2, R, F, E1, E2>(f: F) -> Box<impl Function>
where
    A1: Send + Sync,
    A2: Send + Sync,
    Value: TryInto<A1, Error = E1>,
    Value: TryInto<A2, Error = E2>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    E1: Sync + Send,
    E2: Sync + Send,
    F: Fn(A1, A2) -> R + Sync + Send,
{
    Box::new(BinaryFunctionWrapper::new(f))
}
