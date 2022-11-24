use std::marker::PhantomData;

use crate::pipeline::{PiperError, Value, ValueType, ValueTypeOf};

use super::Function;

pub struct NullaryFunctionWrapper<R, F>
where
    R: Into<Value> + Sync + Send + ValueTypeOf,
    F: Fn() -> R,
{
    pub function: F,
    pub output_type: PhantomData<R>,
}

impl<R, F> NullaryFunctionWrapper<R, F>
where
    R: Into<Value> + Sync + Send + ValueTypeOf,
    F: Fn() -> R,
{
    pub fn new(function: F) -> Self {
        Self {
            function,
            output_type: PhantomData,
        }
    }

    pub fn invoke(&self) -> Value {
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

pub struct UnaryFunctionWrapper<A, R, F, E>
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
    pub fn new(function: F) -> Self {
        Self {
            function,
            _phantom: PhantomData,
        }
    }

    pub fn invoke(&self, args: &[Value]) -> Value {
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

pub struct BinaryFunctionWrapper<A1, A2, R, F, E1, E2>
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
    pub fn new(function: F) -> Self {
        Self {
            function,
            _phantom: PhantomData,
        }
    }

    pub fn invoke(&self, args: &[Value]) -> Value {
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

pub fn nullary_fn<R, F>(f: F) -> Box<NullaryFunctionWrapper<R, F>>
where
    R: Into<Value> + Sync + Send + ValueTypeOf,
    F: Fn() -> R,
{
    Box::new(NullaryFunctionWrapper::new(f))
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
