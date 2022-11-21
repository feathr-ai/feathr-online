use crate::pipeline::{operator::*, PiperError};

use super::OperatorBuilder;

#[derive(Clone, Debug)]
pub struct BinaryOperatorBuilder {
    pub op: String,
}

impl BinaryOperatorBuilder {
    pub fn create<T>(op: T) -> Box<dyn OperatorBuilder>
    where
        T: ToString,
    {
        Box::new(Self { op: op.to_string() })
    }
}

impl OperatorBuilder for BinaryOperatorBuilder {
    fn build(&self) -> Result<Box<dyn Operator>, PiperError> {
        Ok(match self.op.as_str() {
            "+" => Box::new(PlusOperator),
            "-" => Box::new(MinusOperator),
            "*" => Box::new(MultiplyOperator),
            "/" => Box::new(DivideOperator),
            ">" => Box::new(GreaterThanOperator),
            "<" => Box::new(LessThanOperator),
            ">=" => Box::new(GreaterEqualOperator),
            "<=" => Box::new(LessEqualOperator),
            "==" => Box::new(EqualOperator),
            "!=" => Box::new(NotEqualOperator),
            "and" => Box::new(AndOperator),
            "or" => Box::new(OrOperator),
            "index" => Box::new(ArrayIndexOperator),
            "dot" => Box::new(MapIndexOperator),
            _ => Err(PiperError::UnknownOperator(self.op.clone()))?,
        })
    }
}
