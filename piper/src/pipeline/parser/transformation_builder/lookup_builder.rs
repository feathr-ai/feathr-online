use crate::pipeline::{
    parser::expression_builders::ExpressionBuilder,
    transformation::{LookupTransformation, Transformation},
    PiperError, Schema, ValueType, pipelines::BuildContext,
};

use super::TransformationBuilder;

pub struct LookupTransformationBuilder {
    fields: Vec<(String, Option<String>, ValueType)>,
    source: String,
    key: Box<dyn ExpressionBuilder>,
}

impl LookupTransformationBuilder {
    pub fn new(
        fields: Vec<(String, Option<String>, ValueType)>,
        source: String,
        key: Box<dyn ExpressionBuilder>,
    ) -> Box<Self> {
        Box::new(Self {
            fields,
            source,
            key,
        })
    }
}

impl TransformationBuilder for LookupTransformationBuilder {
    fn build(&self, input_schema: &Schema, ctx: &BuildContext) -> Result<Box<dyn Transformation>, PiperError> {
        LookupTransformation::create(
            input_schema,
            self.source.clone(),
            ctx.get_lookup_source(&self.source)?,
            self.fields.clone(),
            self.key.build(input_schema, ctx)?,
        ) as Result<Box<dyn Transformation>, PiperError>
    }
}
