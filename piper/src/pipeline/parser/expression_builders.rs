use std::fmt::Debug;

use crate::pipeline::pipelines::BuildContext;

use super::{
    super::expression::{ColumnExpression, Expression, LiteralExpression, OperatorExpression},
    super::{PiperError, Schema, Value},
    operator_builder::OperatorBuilder,
};

pub trait ExpressionBuilder: Debug {
    fn build(
        &self,
        schema: &Schema,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Expression>, PiperError>;
}

#[derive(Debug)]
pub struct ColumnExpressionBuilder {
    pub column_name: String,
}

impl ColumnExpressionBuilder {
    pub fn create<T>(column_name: T) -> Box<dyn ExpressionBuilder>
    where
        T: ToString,
    {
        Box::new(Self {
            column_name: column_name.to_string(),
        })
    }
}

impl ExpressionBuilder for ColumnExpressionBuilder {
    fn build(
        &self,
        schema: &Schema,
        _ctx: &BuildContext,
    ) -> Result<Box<dyn Expression>, PiperError> {
        let column_index = schema
            .get_column_index(&self.column_name)
            .ok_or_else(|| PiperError::ColumnNotFound(self.column_name.clone()))?;
        Ok(Box::new(ColumnExpression {
            column_name: self.column_name.to_owned(),
            column_index,
        }))
    }
}

#[derive(Debug)]
pub struct LiteralExpressionBuilder {
    pub value: Value,
}

impl LiteralExpressionBuilder {
    pub fn create<T>(value: T) -> Box<dyn ExpressionBuilder>
    where
        Value: From<T>,
    {
        Box::new(Self {
            value: value.into(),
        })
    }
}

impl ExpressionBuilder for LiteralExpressionBuilder {
    fn build(
        &self,
        _schema: &Schema,
        _ctx: &BuildContext,
    ) -> Result<Box<dyn Expression>, PiperError> {
        Ok(Box::new(LiteralExpression {
            value: self.value.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct OperatorExpressionBuilder {
    pub operator: Box<dyn OperatorBuilder>,
    pub arguments: Vec<Box<dyn ExpressionBuilder>>,
}

impl OperatorExpressionBuilder {
    pub fn create(
        operator: Box<dyn OperatorBuilder>,
        arguments: Vec<Box<dyn ExpressionBuilder>>,
    ) -> Box<dyn ExpressionBuilder> {
        Box::new(Self {
            operator,
            arguments,
        })
    }
}

impl ExpressionBuilder for OperatorExpressionBuilder {
    fn build(
        &self,
        schema: &Schema,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Expression>, PiperError> {
        let arguments = self
            .arguments
            .iter()
            .map(|e| e.build(schema, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Box::new(OperatorExpression {
            operator: self.operator.build(ctx)?,
            arguments,
        }))
    }
}
