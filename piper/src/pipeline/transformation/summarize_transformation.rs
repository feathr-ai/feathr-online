use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;

use crate::{
    pipeline::{aggregation::Aggregation, expression::Expression, Column, DataSet, Schema},
    PiperError, Value,
};

use super::Transformation;

#[derive(Debug, Clone)]
struct Agg {
    column_name: String,
    aggregation: Aggregation,
}

#[derive(Debug)]
struct Key {
    column_name: String,
    expression: Box<dyn Expression>,
}

#[derive(Debug)]
pub struct SummarizeTransformation {
    output_schema: Schema,
    aggregations: Vec<Agg>,
    keys: Arc<Vec<Key>>,
}

// summarize col1=agg1(param1, param2), col2=agg2(param1, param2) [by key1[=expr1], key2[=expr2]]
impl SummarizeTransformation {
    pub fn create(
        input_schema: &Schema,
        aggs: Vec<(String, Aggregation)>,
        keys: Vec<(String, Option<Box<dyn Expression>>)>,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        let aggregations: Vec<Agg> = aggs
            .into_iter()
            .map(|(col, agg)| Agg {
                column_name: col,
                aggregation: agg,
            })
            .collect();
        let keys: Vec<Key> = keys
            .into_iter()
            .map(|(col, expr)| {
                Ok(Key {
                    expression: match expr {
                        Some(expr) => expr,
                        None => input_schema.get_col_expr(&col)?,
                    },
                    column_name: col,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut columns = vec![];
        for agg in aggregations.iter() {
            let agg_type = agg
                .aggregation
                .get_output_type(&input_schema.get_column_types())?;
            columns.push(Column::new(agg.column_name.clone(), agg_type));
        }
        for key in keys.iter() {
            let col = Column::new(
                key.column_name.clone(),
                key.expression
                    .get_output_type(&input_schema.get_column_types())?,
            );
            columns.push(col);
        }
        let output_schema = Schema::from(columns);
        Ok(Box::new(SummarizeTransformation {
            output_schema,
            aggregations,
            keys: Arc::new(keys),
        }))
    }
}

impl Transformation for SummarizeTransformation {
    fn get_output_schema(&self, _input_schema: &Schema) -> Schema {
        self.output_schema.clone()
    }

    fn transform(&self, dataset: Box<dyn DataSet>) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(SummarizedDataSet {
            input: dataset,
            output_schema: self.output_schema.clone(),
            aggregations: self.aggregations.clone(),
            keys: self.keys.clone(),
            rows: None,
        }))
    }

    fn dump(&self) -> String {
        let aggs = self
            .aggregations
            .iter()
            .map(|agg| format!("{}={}", agg.column_name, agg.aggregation.dump()))
            .collect::<Vec<_>>()
            .join(", ");
        let keys = self
            .keys
            .iter()
            .map(|key| format!("{}={}", key.column_name, key.expression.dump()))
            .collect::<Vec<_>>()
            .join(", ");
        format!("summarize {} by {}", aggs, keys)
    }
}

struct SummarizedDataSet {
    input: Box<dyn DataSet>,
    output_schema: Schema,
    aggregations: Vec<Agg>,
    keys: Arc<Vec<Key>>,
    rows: Option<VecDeque<Vec<Value>>>,
}

impl SummarizedDataSet {
    fn get_key(&self, row: &[Value]) -> Vec<Value> {
        self.keys.iter().map(|k| k.expression.eval(row)).collect()
    }

    async fn do_fetch_rows(&mut self) -> Result<(), PiperError> {
        if self.rows.is_none() {
            let mut key_to_agg: HashMap<Vec<Value>, Vec<Agg>> = HashMap::new();
            while let Some(row) = self.input.next().await {
                let key = self.get_key(&row);
                match key_to_agg.entry(key) {
                    std::collections::hash_map::Entry::Occupied(o) => {
                        let agg = o.into_mut();
                        agg.iter_mut()
                            .try_for_each(|agg| agg.aggregation.feed(&row))?;
                    }
                    std::collections::hash_map::Entry::Vacant(v) => {
                        let mut agg = self.aggregations.clone();
                        agg.iter_mut()
                            .try_for_each(|agg| agg.aggregation.feed(&row))?;
                        v.insert(agg);
                    }
                };
            }
            let mut rows = VecDeque::new();
            for (key, agg) in key_to_agg.into_iter() {
                let mut row = vec![];
                for agg in agg.into_iter() {
                    row.push(agg.aggregation.get_result()?);
                }
                row.extend(key);
                rows.push_back(row);
            }
            self.rows = Some(rows);
        }
        Ok(())
    }

    async fn fetch_rows(&mut self) {
        match self.do_fetch_rows().await {
            Ok(_) => {}
            Err(e) => {
                let mut rows = VecDeque::new();
                // Propagate error to all fields in the only row.
                rows.push_back(vec![e.into(); self.output_schema.columns.len()]);
                self.rows = Some(rows);
            }
        }
    }
}

#[async_trait]
impl DataSet for SummarizedDataSet {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        if self.rows.is_none() {
            self.fetch_rows().await;
        }
        self.rows.as_mut()?.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{parser::parse_pipeline, BuildContext, Column, DataSetCreator, Schema};

    #[tokio::test]
    async fn test_summarize() {
        let p = r#"t(x,y,z)
        | summarize a=count(), sx=sum(x), sz=sum(z) by y
        ;"#;
        let ctx = BuildContext::default();
        let p = parse_pipeline(p, &ctx).unwrap();
        let dataset = DataSetCreator::eager(
            Schema::from(vec![
                Column::new("x", crate::ValueType::Int),
                Column::new("y", crate::ValueType::Int),
                Column::new("z", crate::ValueType::Int),
            ]),
            vec![
                vec![42.into(), 1.into(), 12.into()],
                vec![37.into(), 2.into(), 13.into()],
                vec![56.into(), 3.into(), 14.into()],
                vec![89.into(), 2.into(), 15.into()],
                vec![13.into(), 3.into(), 16.into()],
                vec![24.into(), 3.into(), 17.into()],
            ],
        );
        let mut ret = p
            .process(dataset, crate::pipeline::ValidationMode::Lenient)
            .unwrap();
        let (_, rows) = ret.eval().await;
        println!("{:?}", rows);

        assert_eq!(rows.len(), 3);

        let r1 = rows.iter().find(|r| r[3] == 1.into()).unwrap();
        let r2 = rows.iter().find(|r| r[3] == 2.into()).unwrap();
        let r3 = rows.iter().find(|r| r[3] == 3.into()).unwrap();
        assert_eq!(r1.len(), 4);
        assert_eq!(r2.len(), 4);
        assert_eq!(r3.len(), 4);

        assert_eq!(r1[0], 1.into());
        assert_eq!(r1[1], 42.into());
        assert_eq!(r1[2], 12.into());
        assert_eq!(r1[3], 1.into());

        assert_eq!(r2[0], 2.into());
        assert_eq!(r2[1], (37 + 89).into());
        assert_eq!(r2[2], (13 + 15).into());
        assert_eq!(r2[3], 2.into());

        assert_eq!(r3[0], 3.into());
        assert_eq!(r3[1], (56 + 13 + 24).into());
        assert_eq!(r3[2], (14 + 16 + 17).into());
        assert_eq!(r3[3], 3.into());
    }
}
