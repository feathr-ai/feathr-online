use std::marker::PhantomData;

use crate::pipeline::{PiperError, Value, ValueType, ValueTypeOf};

use super::Function;

#[derive(Clone)]
pub struct VariadicFunctionWrapper<A, R, F, E>
where
    A: Send + Sync + Clone + TryFrom<Value, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    Result<Value, E>: Into<Value>,
    E: Sync + Send + Clone,
    F: Fn(Vec<A>) -> R + Clone,
{
    function: F,
    _phantom: PhantomData<(A, R, E)>,
}

impl<A, R, F, E> Function for VariadicFunctionWrapper<A, R, F, E>
where
    A: Send + Sync + Clone + TryFrom<Value, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    Result<Value, E>: Into<Value>,
    E: Sync + Send + Clone,
    F: Fn(Vec<A>) -> R + Sync + Send + Clone,
{
    fn get_output_type(&self, _argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        Ok(R::value_type())
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        let args = arguments
            .into_iter()
            .map(|arg| arg.try_into())
            .collect::<Result<Vec<A>, E>>();
        match args {
            Ok(args) => (self.function)(args).into(),
            Err(e) => Err(e).into(),
        }
    }
}

pub fn var_fn<A, R, F, E>(f: F) -> Box<impl Function>
where
    A: Send + Sync + Clone + TryFrom<Value, Error = E>,
    R: Into<Value> + Sync + Send + ValueTypeOf + Clone,
    F: Fn(Vec<A>) -> R + Sync + Send + Clone,
    Result<Value, E>: Into<Value>,
    E: Sync + Send + Clone,
{
    Box::new(VariadicFunctionWrapper {
        function: f,
        _phantom: PhantomData,
    })
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
        let f = var_fn(|args: Vec<Value>| {
            args.into_iter()
                .find(|v| !v.is_null())
                .unwrap_or(Value::Null)
        });
        assert_eq!(
            f.eval(vec![Value::Null, 42.into(), 2.into(), 3.into()]),
            42.into()
        );
    }
}
