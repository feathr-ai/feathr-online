use crate::pipeline::{operator::*, PiperError};

use super::OperatorBuilder;

#[derive(Clone, Debug)]
pub struct UnaryOperatorBuilder {
    pub op: String,
}

impl UnaryOperatorBuilder {
    pub fn new<T>(op: T) -> Box<dyn OperatorBuilder>
    where
        T: ToString,
    {
        Box::new(Self { op: op.to_string() })
    }
}

impl OperatorBuilder for UnaryOperatorBuilder {
    fn build(&self) -> Result<Box<dyn Operator>, PiperError> {
        Ok(match self.op.as_str() {
            "+" => Box::new(PositiveOperator),
            "-" => Box::new(NegativeOperator),
            "not" => Box::new(NotOperator),
            "is null" => Box::new(IsNullOperator),
            "is not null" => Box::new(IsNotNullOperator),
            _ => Err(PiperError::UnknownOperator(self.op.clone()))?,
        })
    }
}
