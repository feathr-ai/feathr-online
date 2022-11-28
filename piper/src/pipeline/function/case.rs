use tracing::instrument;

use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Clone, Debug)]
pub struct CaseFunction;

impl Function for CaseFunction {
    fn get_output_type(
        &self,
        argument_types: &[ValueType],
    ) -> Result<ValueType, PiperError> {
        if argument_types.is_empty() {
            return Err(PiperError::InvalidArgumentCount(1, 9));
        }
        let last_result_type = argument_types.last().unwrap();
        for (idx, pair) in argument_types.chunks(2).enumerate() {
            let case_type = if pair.len() == 1 {
                pair[0]
            } else {
                if pair[0] != ValueType::Bool && pair[0] != ValueType::Dynamic {
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
        Ok(*last_result_type)
    }

    #[instrument(level = "trace", skip(self))]
    fn eval(&self, arguments: Vec<Value>) -> Value {
        for pair in arguments.chunks(2) {
            if pair.len() == 1 {
                // Default case
                return pair[0].clone();
            }
            match pair[0].get_bool() {
                Ok(true) => return pair[1].clone(),
                Ok(false) => continue,
                Err(e) => return e.into(),
            }
        }
        // No default case, and no case matched
        Value::Null
    }
}
