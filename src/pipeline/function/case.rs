use tracing::instrument;

use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Clone, Debug)]
pub struct CaseFunction;

impl Function for CaseFunction {
    fn get_output_type(
        &self,
        argument_types: &[ValueType],
    ) -> Result<crate::pipeline::ValueType, crate::pipeline::PiperError> {
        if argument_types.len() == 0 {
            return Err(PiperError::InvalidArgumentCount(1, 9));
        }
        let last_result_type = argument_types.last().unwrap();
        for (idx, pair) in argument_types.chunks(2).enumerate() {
            let case_type = if pair.len() == 1 {
                pair[0]
            } else {
                if pair[0] != ValueType::Bool {
                    return Err(PiperError::InvalidArgumentType(
                        "case".to_string(),
                        idx,
                        pair[0],
                    ));
                }
                pair[1]
            };
            if &case_type != last_result_type {
                return Ok(ValueType::Dynamic);
            }
        }
        Ok(last_result_type.clone())
    }

    #[instrument(level = "trace", skip(self))]
    fn eval(
        &self,
        arguments: Vec<Value>,
    ) -> Result<crate::pipeline::Value, crate::pipeline::PiperError> {
        for pair in arguments.chunks(2) {
            if pair.len() == 1 {
                // Default case
                return Ok(pair[0].clone());
            }
            if pair[0].get_bool()? {
                return Ok(pair[1].clone());
            }
        }
        // No default case, and no case matched
        return Ok(Value::Null);
    }
}
