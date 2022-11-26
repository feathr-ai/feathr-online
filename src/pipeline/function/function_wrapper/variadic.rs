use std::marker::PhantomData;

use crate::pipeline::{PiperError, Value, ValueType, ValueTypeOf};

use super::Function;
pub struct VariadicFunctionWrapper<A, R, F, E>
where
    A: Send + Sync,
    Value: TryInto<A, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    Result<Value, E>: Into<Value>,
    E: Sync + Send,
    F: Fn(Vec<A>) -> R,
{
    function: F,
    _phantom: PhantomData<(A, R, E)>,
}

impl<A, R, F, E> VariadicFunctionWrapper<A, R, F, E>
where
    A: Send + Sync,
    Value: TryInto<A, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    Result<Value, E>: Into<Value>,
    E: Sync + Send,
    F: Fn(Vec<A>) -> R,
{
    pub fn new(function: F) -> Self {
        Self {
            function,
            _phantom: PhantomData,
        }
    }

    pub fn invoke(&self, args: &[Value]) -> Value {
        args.iter()
            .map(|arg| arg.clone().try_into())
            .collect::<Result<Vec<A>, E>>()
            .map(|a| (self.function)(a))
            .map(|r| r.into())
            .into()
    }
}

impl<A, R, F, E> Function for VariadicFunctionWrapper<A, R, F, E>
where
    A: Send + Sync,
    Value: TryInto<A, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    Result<Value, E>: Into<Value>,
    E: Sync + Send,
    F: Fn(Vec<A>) -> R + Sync + Send,
{
    fn get_output_type(&self, _argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        Ok(R::value_type())
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        self.invoke(&arguments)
    }
}

pub fn var_fn<A, R, F, E>(f: F) -> Box<impl Function>
where
    A: Send + Sync,
    Value: TryInto<A, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf,
    F: Fn(Vec<A>) -> R + Sync + Send,
    Result<Value, E>: Into<Value>,
    E: Sync + Send,
{
    Box::new(VariadicFunctionWrapper::new(f))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variadic_function_wrapper() {
        let f = var_fn(|args: Vec<i32>| args.iter().sum::<i32>());
        assert_eq!(f.eval(vec![1.into(), 2.into(), 3.into()]), 6.into());
    }

    #[test]
    fn test_coalesce() {
        let f = var_fn(|args: Vec<Value>| args.into_iter().find(|v| !v.is_null()).unwrap_or(Value::Null));
        assert_eq!(f.eval(vec![Value::Null, 42.into(), 2.into(), 3.into()]), 42.into());
    }
}