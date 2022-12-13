use std::sync::Arc;

use crate::{
    pipeline::{
        parser::expression_builders::ExpressionBuilder,
        transformation::{DistinctTransformation, Transformation},
        BuildContext, Schema,
    },
    PiperError,
};

use super::TransformationBuilder;

#[derive(Debug)]
pub struct DistinctTransformationBuilder {
    pub keys: Vec<(String, Option<Box<dyn ExpressionBuilder>>)>,
}

impl TransformationBuilder for DistinctTransformationBuilder {
    fn build(
        &self,
        input_schema: &Schema,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        let key_fields = if self.keys.is_empty() {
            input_schema
                .columns
                .iter()
                .map(|c| input_schema.get_col_expr(&c.name))
                .collect::<Result<Vec<_>, PiperError>>()?
        } else {
            self.keys
                .iter()
                .map(|(name, expr)| {
                    expr.as_ref().map_or_else(
                        || input_schema.get_col_expr(name),
                        |e| e.build(input_schema, ctx),
                    )
                })
                .collect::<Result<Vec<_>, PiperError>>()?
        };

        let output_schema = if self.keys.is_empty() {
            input_schema.clone()
        } else {
            Schema::from(
                self.keys
                    .iter()
                    .map(|(c, _)| {
                        input_schema
                            .get_column_index(c)
                            .ok_or_else(|| PiperError::ColumnNotFound(c.to_string()))
                            .map(|i| input_schema.columns[i].clone())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        };

        Ok(Box::new(DistinctTransformation {
            key_fields: Arc::new(key_fields),
            output_schema,
        }))
    }
}
