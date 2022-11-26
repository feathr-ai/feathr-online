use std::marker::PhantomData;

use crate::pipeline::{PiperError, Value, ValueType, ValueTypeOf};

use super::Function;

struct UnaryFunctionWrapper<A, R, F, E>
where
    A: Send + Sync,
    Value: TryInto<A, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    Result<Value, E>: Into<Value>,
    E: Sync + Send,
    F: Fn(A) -> R,
{
    function: F,
    _phantom: PhantomData<(A, R, E)>,
}

impl<A, R, F, E> UnaryFunctionWrapper<A, R, F, E>
where
    A: Send + Sync,
    Value: TryInto<A, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    Result<Value, E>: Into<Value>,
    E: Sync + Send,
    F: Fn(A) -> R,
{
    fn new(function: F) -> Self {
        Self {
            function,
            _phantom: PhantomData,
        }
    }

    fn invoke(&self, args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Error(PiperError::InvalidArgumentCount(1, args.len()));
        }

        match args[0].clone().try_into() {
            Ok(a) => (self.function)(a).into(),
            Err(e) => Err(e).into(),
        }
    }
}

impl<A, R, F, E> Function for UnaryFunctionWrapper<A, R, F, E>
where
    A: Send + Sync,
    Value: TryInto<A, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    F: (Fn(A) -> R) + Sync + Send,
    Result<Value, E>: Into<Value>,
    E: Sync + Send,
{
    fn get_output_type(&self, _argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        Ok(R::value_type())
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        self.invoke(&arguments)
    }
}

pub fn unary_fn<A, R, F, E>(f: F) -> Box<impl Function>
where
    A: Send + Sync,
    Value: TryInto<A, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    F: Fn(A) -> R + Sync + Send,
    Result<Value, E>: Into<Value>,
    E: Sync + Send,
{
    Box::new(UnaryFunctionWrapper::new(f))
}
