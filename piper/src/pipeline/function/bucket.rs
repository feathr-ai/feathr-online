use tracing::instrument;

use crate::pipeline::{
    operator::{LessThanOperator, Operator},
    PiperError, Value, ValueType,
};

use super::Function;

#[derive(Clone, Debug)]
pub struct BucketFunction;

impl Function for BucketFunction {
    fn get_output_type(
        &self,
        argument_types: &[ValueType],
    ) -> Result<ValueType, PiperError> {
        if argument_types.len() < 2 {
            return Err(PiperError::InvalidArgumentCount(2, argument_types.len()));
        }
        let param_type = argument_types[0];
        for (idx, pivot_type) in argument_types.iter().enumerate().skip(1) {
            if LessThanOperator
                .get_output_type(&[param_type, *pivot_type])
                .is_err()
            {
                return Err(PiperError::InvalidArgumentType(
                    "bucket".to_string(),
                    idx,
                    *pivot_type,
                ));
            }
        }
        Ok(ValueType::Long)
    }

    #[instrument(level = "trace", skip(self))]
    fn eval(&self, arguments: Vec<Value>) -> Value {
        for (bucket, pivot) in arguments.iter().enumerate().skip(1) {
            let pred = LessThanOperator
                .eval(vec![arguments[0].clone(), pivot.clone()])
                .get_bool();
            match pred {
                Ok(true) => return bucket.into(),
                Ok(false) => continue,
                Err(e) => return e.into(),
            }
        }
        (arguments.len() - 1).into()
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{
        function::{bucket::BucketFunction, Function},
        ValueType,
    };

    #[test]
    fn test_bucket_type() {
        assert_eq!(
            BucketFunction
                .get_output_type(&[
                    ValueType::Int,
                    ValueType::Float,
                    ValueType::Float,
                    ValueType::Float
                ])
                .unwrap(),
            ValueType::Long
        );

        assert_eq!(
            BucketFunction
                .get_output_type(&[
                    ValueType::Double,
                    ValueType::Float,
                    ValueType::Float,
                    ValueType::Float
                ])
                .unwrap(),
            ValueType::Long
        );
    }
}
