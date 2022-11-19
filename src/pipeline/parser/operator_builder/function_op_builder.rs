use crate::pipeline::{operator::*, PiperError, function::get_function};

use super::OperatorBuilder;


#[derive(Clone, Debug)]
pub struct FunctionOperatorBuilder {
    pub name: String,
}

impl FunctionOperatorBuilder {
    pub fn new<T>(name: T) -> Box<dyn OperatorBuilder>
    where
        T: ToString,
    {
        Box::new(Self {
            name: name.to_string(),
        })
    }
}

impl OperatorBuilder for FunctionOperatorBuilder {
    fn build(&self) -> Result<Box<dyn Operator>, PiperError> {
        match get_function(&self.name) {
            Some((name, function)) => Ok(Box::new(FunctionOperator { name, function })),
            None => Err(PiperError::UnknownFunction(self.name.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{parser::{expression_builders::{OperatorExpressionBuilder, LiteralExpressionBuilder}, operator_builder::FunctionOperatorBuilder}, Schema};

    #[test]
    fn test_build() {
        let schema = Schema::new();
        let operator = FunctionOperatorBuilder::new("bucket");
        let expression = OperatorExpressionBuilder::new(
            operator,
            vec![
                LiteralExpressionBuilder::new(1),
                LiteralExpressionBuilder::new(2),
            ],
        );
        let _ = expression.build(&schema).unwrap();
    }
}