use crate::pipeline::{operator::*, pipelines::BuildContext, PiperError};

use super::OperatorBuilder;

#[derive(Clone, Debug)]
pub struct FunctionOperatorBuilder {
    pub name: String,
}

impl FunctionOperatorBuilder {
    pub fn create<T>(name: T) -> Box<dyn OperatorBuilder>
    where
        T: ToString,
    {
        Box::new(Self {
            name: name.to_string(),
        })
    }
}

impl OperatorBuilder for FunctionOperatorBuilder {
    fn build(&self, ctx: &BuildContext) -> Result<Box<dyn Operator>, PiperError> {
        // match get_function(&self.name) {
        match ctx.functions.get(&self.name) {
            Some(function) => Ok(Box::new(FunctionOperator {
                name: self.name.clone(),
                function: crate::common::IgnoreDebug {
                    inner: function.clone(),
                },
            })),
            None => Err(PiperError::UnknownFunction(self.name.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{
        parser::{
            expression_builders::{LiteralExpressionBuilder, OperatorExpressionBuilder},
            operator_builder::FunctionOperatorBuilder,
        },
        Schema, pipelines::BuildContext,
    };

    #[test]
    fn test_build() {
        let schema = Schema::default();
        let operator = FunctionOperatorBuilder::create("pow");
        let expression = OperatorExpressionBuilder::create(
            operator,
            vec![
                LiteralExpressionBuilder::create(1),
                LiteralExpressionBuilder::create(2),
            ],
        );
        let _ = expression
            .build(&schema, &BuildContext::default())
            .unwrap();
    }
}
