use crate::pipeline::{
    transformation::{ExplodeTransformation, Transformation},
    PiperError, Schema, ValueType, pipelines::BuildContext,
};

use super::TransformationBuilder;

pub struct ExplodeTransformationBuilder {
    pub column: String,
    pub exploded_type: ValueType,
}

impl ExplodeTransformationBuilder {
    pub fn create(column: String, exploded_type: Option<ValueType>) -> Box<dyn TransformationBuilder> {
        Box::new(Self {
            column,
            exploded_type: exploded_type.unwrap_or(ValueType::Dynamic),
        })
    }
}

impl TransformationBuilder for ExplodeTransformationBuilder {
    fn build(&self, input_schema: &Schema, _ctx: &BuildContext) -> Result<Box<dyn Transformation>, PiperError> {
        let column_idx = input_schema
            .columns
            .iter()
            .position(|c| c.name == self.column)
            .ok_or_else(|| {
                PiperError::ColumnNotFound(self.column.clone())
            })?;

        Ok(ExplodeTransformation::create(
            input_schema,
            column_idx,
            self.exploded_type,
        ))
    }
}
