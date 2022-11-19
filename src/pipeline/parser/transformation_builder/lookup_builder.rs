use crate::pipeline::{
    lookup::get_lookup_source,
    parser::expression_builders::ExpressionBuilder,
    transformation::{LookupTransformation, Transformation},
    PiperError, Schema, ValueType,
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
    ) -> Box<dyn TransformationBuilder> {
        Box::new(Self {
            fields,
            source,
            key,
        })
    }
}

impl TransformationBuilder for LookupTransformationBuilder {
    fn build(&self, input_schema: &Schema) -> Result<Box<dyn Transformation>, PiperError> {
        LookupTransformation::new(
            input_schema,
            self.source.clone(),
            get_lookup_source(&self.source)?,
            self.fields.clone(),
            self.key.build(input_schema)?,
        )
    }
}
