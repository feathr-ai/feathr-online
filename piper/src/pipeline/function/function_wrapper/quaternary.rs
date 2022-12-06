use std::marker::PhantomData;

use crate::pipeline::{PiperError, Value, ValueType, ValueTypeOf};

use super::Function;

#[derive(Clone)]
struct QuaternaryFunctionWrapper<A1, A2, A3, A4, R, F, E1, E2, E3, E4>
where
    A1: Send + Sync + Clone,
    A2: Send + Sync + Clone,
    A3: Send + Sync + Clone,
    A4: Send + Sync + Clone,
    Value: TryInto<A1, Error = E1>,
    Value: TryInto<A2, Error = E2>,
    Value: TryInto<A3, Error = E3>,
    Value: TryInto<A4, Error = E4>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    Result<Value, E3>: Into<Value>,
    Result<Value, E4>: Into<Value>,
    E1: Sync + Send + Clone,
    E2: Sync + Send + Clone,
    E3: Sync + Send + Clone,
    E4: Sync + Send + Clone,
    F: Fn(A1, A2, A3, A4) -> R + Clone,
{
    function: F,
    #[allow(clippy::type_complexity)]
    _phantom: PhantomData<(A1, A2, A3, A4, R, E1, E2, E3, E4)>,
}

impl<A1, A2, A3, A4, R, F, E1, E2, E3, E4> QuaternaryFunctionWrapper<A1, A2, A3, A4, R, F, E1, E2, E3, E4>
where
    A1: Send + Sync + Clone,
    A2: Send + Sync + Clone,
    A3: Send + Sync + Clone,
    A4: Send + Sync + Clone,
    Value: TryInto<A1, Error = E1>,
    Value: TryInto<A2, Error = E2>,
    Value: TryInto<A3, Error = E3>,
    Value: TryInto<A4, Error = E4>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    Result<Value, E3>: Into<Value>,
    Result<Value, E4>: Into<Value>,
    E1: Sync + Send + Clone,
    E2: Sync + Send + Clone,
    E3: Sync + Send + Clone,
    E4: Sync + Send + Clone,
    F: Fn(A1, A2, A3, A4) -> R + Clone,
{
    fn new(function: F) -> Self {
        Self {
            function,
            _phantom: PhantomData,
        }
    }

    fn invoke(&self, args: &[Value]) -> Value {
        if args.len() != 4 {
            return Value::Error(PiperError::InvalidArgumentCount(4, args.len()));
        }

        let a1: Result<A1, E1> = args[0].clone().try_into();
        let a2: Result<A2, E2> = args[1].clone().try_into();
        let a3: Result<A3, E3> = args[2].clone().try_into();
        let a4: Result<A4, E4> = args[3].clone().try_into();

        match (a1, a2, a3, a4) {
            (Ok(a1), Ok(a2), Ok(a3), Ok(a4)) => (self.function)(a1, a2, a3, a4).into(),
            (Err(e), _, _, _) => Err(e).into(),
            (_, Err(e), _, _) => Err(e).into(),
            (_, _, Err(e), _) => Err(e).into(),
            (_, _, _, Err(e)) => Err(e).into(),
        }
    }
}

impl<A1, A2, A3, A4, R, F, E1, E2, E3, E4> Function for QuaternaryFunctionWrapper<A1, A2, A3, A4, R, F, E1, E2, E3, E4>
where
    A1: Send + Sync + Clone,
    A2: Send + Sync + Clone,
    A3: Send + Sync + Clone,
    A4: Send + Sync + Clone,
    Value: TryInto<A1, Error = E1>,
    Value: TryInto<A2, Error = E2>,
    Value: TryInto<A3, Error = E3>,
    Value: TryInto<A4, Error = E4>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    F: Fn(A1, A2, A3, A4) -> R + Sync + Send + Clone,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    Result<Value, E3>: Into<Value>,
    Result<Value, E4>: Into<Value>,
    E1: Sync + Send + Clone,
    E2: Sync + Send + Clone,
    E3: Sync + Send + Clone,
    E4: Sync + Send + Clone,
{
    fn get_output_type(&self, _argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        Ok(R::value_type())
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        self.invoke(&arguments)
    }
}

pub fn quaternary_fn<A1, A2, A3, A4, R, F, E1, E2, E3, E4>(f: F) -> Box<impl Function>
where
    A1: Send + Sync + Clone,
    A2: Send + Sync + Clone,
    A3: Send + Sync + Clone,
    A4: Send + Sync + Clone,
    Value: TryInto<A1, Error = E1>,
    Value: TryInto<A2, Error = E2>,
    Value: TryInto<A3, Error = E3>,
    Value: TryInto<A4, Error = E4>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    Result<Value, E1>: Into<Value>,
    Result<Value, E2>: Into<Value>,
    Result<Value, E3>: Into<Value>,
    Result<Value, E4>: Into<Value>,
    E1: Sync + Send + Clone,
    E2: Sync + Send + Clone,
    E3: Sync + Send + Clone,
    E4: Sync + Send + Clone,
    F: Fn(A1, A2, A3, A4) -> R + Sync + Send + Clone,
{
    Box::new(QuaternaryFunctionWrapper::new(f))
}
