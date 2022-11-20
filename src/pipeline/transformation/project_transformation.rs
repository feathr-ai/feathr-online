use async_trait::async_trait;

use crate::pipeline::{expression::Expression, Column, DataSet, PiperError, Value};

use super::Transformation;

#[derive(Clone, Debug)]
pub struct ProjectTransformation {
    output_schema: crate::pipeline::Schema,
    columns: Vec<Box<dyn Expression>>,
    column_names: Vec<String>,
}

impl ProjectTransformation {
    pub fn new(
        input_schema: &crate::pipeline::Schema,
        columns: Vec<(String, Box<dyn Expression>)>,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        let column_names = columns.iter().map(|(c, _)| c.clone()).collect();
        let addition_columns = columns
            .iter()
            .map(|(name, exp)| {
                let column_type = exp.get_output_type(&input_schema.get_column_types())?;
                Ok(Column {
                    name: name.clone(),
                    column_type,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut col_expr = input_schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                Box::new(crate::pipeline::expression::ColumnExpression {
                    column_index: i,
                    column_name: c.name.clone(),
                }) as Box<dyn Expression>
            })
            .collect::<Vec<_>>();
        let addition_col_expr = columns
            .iter()
            .map(|(_, exp)| exp.clone())
            .collect::<Vec<_>>();
        col_expr.extend(addition_col_expr.into_iter());
        let output_schema = input_schema
            .columns
            .clone()
            .into_iter()
            .chain(addition_columns)
            .collect();
        Ok(Box::new(Self {
            output_schema,
            columns: col_expr,
            column_names,
        }))
    }
}

impl Transformation for ProjectTransformation {
    fn get_output_schema(
        &self,
        _input_schema: &crate::pipeline::Schema,
    ) -> crate::pipeline::Schema {
        self.output_schema.clone()
    }

    fn transform(
        &self,
        dataset: Box<dyn crate::pipeline::DataSet>,
    ) -> Result<Box<dyn crate::pipeline::DataSet>, crate::pipeline::PiperError> {
        Ok(Box::new(ProjectedDataSet {
            input_dataset: dataset,
            output_schema: self.output_schema.clone(),
            columns: self.columns.clone(),
        }))
    }

    fn dump(&self) -> String {
        format!(
            "project {}",
            self.column_names
                .iter()
                .zip(
                    self.columns
                        .iter()
                        .skip(self.columns.len() - self.column_names.len())
                )
                .map(|(c, e)| format!("{} = {}", c, e.dump()))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

struct ProjectedDataSet {
    input_dataset: Box<dyn crate::pipeline::DataSet>,
    output_schema: crate::pipeline::Schema,
    columns: Vec<Box<dyn Expression>>,
}

#[async_trait]
impl DataSet for ProjectedDataSet {
    fn schema(&self) -> &crate::pipeline::Schema {
        &self.output_schema
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        match self.input_dataset.next().await {
            Some(row) => {
                let mut output_row = Vec::with_capacity(self.columns.len());
                for col in &self.columns {
                    output_row.push(col.eval(&row));
                }
                Some(output_row)
            }
            None => None,
        }
    }
}
