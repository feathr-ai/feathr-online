use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use rust_heap::BoundedBinaryHeap;

use crate::pipeline::{expression::Expression, DataSet, Schema, Value, PiperError};

use super::Transformation;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl Default for SortOrder {
    fn default() -> Self {
        Self::Descending
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullPos {
    First,
    Last,
}

impl Default for NullPos {
    fn default() -> Self {
        Self::Last
    }
}

#[derive(Debug)]
pub struct TopTransformation {
    pub count: usize,
    pub criteria: Arc<dyn Expression>,
    pub sort_order: SortOrder,
    pub null_pos: NullPos,
}

impl TopTransformation {
    pub fn new(
        count: usize,
        criteria: Box<dyn Expression>,
        sort_order: Option<SortOrder>,
        null_pos: Option<NullPos>,
    ) -> Box<Self> {
        Box::new(TopTransformation {
            count,
            criteria: criteria.into(),
            sort_order: sort_order.unwrap_or_default(),
            null_pos: null_pos.unwrap_or_default(),
        })
    }
}

impl Transformation for TopTransformation {
    fn get_output_schema(&self, input_schema: &Schema) -> Schema {
        input_schema.clone()
    }

    fn transform(
        &self,
        dataset: Box<dyn DataSet>,
    ) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(TopDataSet {
            input: dataset,
            count: self.count,
            criteria: self.criteria.clone(),
            sort_order: self.sort_order,
            null_pos: self.null_pos,
            rows: None,
        }))
    }

    fn dump(&self) -> String {
        format!(
            "top {} by {} {} nulls {}",
            self.count,
            self.criteria.dump(),
            match self.sort_order {
                SortOrder::Ascending => "asc",
                SortOrder::Descending => "desc",
            },
            match self.null_pos {
                NullPos::First => "first",
                NullPos::Last => "last",
            }
        )
    }
}

struct TopDataSet {
    input: Box<dyn DataSet>,
    count: usize,
    criteria: Arc<dyn Expression>,
    sort_order: SortOrder,
    null_pos: NullPos,
    rows: Option<VecDeque<Vec<Value>>>,
}

#[derive(Clone, Debug)]
struct SortRow(Value, Vec<Value>, SortOrder);

impl PartialEq for SortRow {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialOrd for SortRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.2 {
            SortOrder::Ascending => self.0.partial_cmp(&other.0).map(|x| x.reverse()),
            SortOrder::Descending => self.0.partial_cmp(&other.0),
        }
    }
}

#[async_trait]
impl DataSet for TopDataSet {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    /// Sorting is an expensive operation, it has to fetch all the rows from the input dataset in order to decide which rows to keep.
    async fn next(&mut self) -> Option<Vec<Value>> {
        // The sorting happens on the 1st time next() is called.
        if self.rows.is_none() {
            // Sort input if we haven't
            self.sort_rows().await;
        }

        match self.rows.as_mut() {
            Some(rows) => rows.pop_front(),
            None => None,
        }
    }
}

impl TopDataSet {
    async fn sort_rows(&mut self) {
        let mut null_rows = Vec::with_capacity(self.count);
        let mut heap = BoundedBinaryHeap::new(self.count);

        while let Some(row) = self.input.next().await {
            let sort_row = SortRow(self.criteria.eval(&row), row, self.sort_order);

            if sort_row.0.is_null() || sort_row.0.is_error() {
                if null_rows.len() < self.count {
                    // We don't really send null rows to the heap, they're stashed in a separate vector.
                    // We only keep at most `count` of null rows, we don't need more no matter the sorting dir.
                    // We treat all null values are equal, so any null row can be used.
                    null_rows.push(sort_row.1);
                }
            } else {
                heap.push(sort_row);
            }
        }

        let mut sorted = vec![];
        for _ in 0..heap.len() {
            sorted.push(heap.pop().unwrap());
        }
        // pop will return the heap root, which is logically the bottom element, so we need to reverse the vector if we want the correct order.
        sorted.reverse();

        let mut ret = vec![];
        match self.null_pos {
            NullPos::First => {
                ret.extend(
                    null_rows
                        .into_iter()
                        .chain(sorted.into_iter().map(|x| x.1))
                        .take(self.count),
                );
            }
            NullPos::Last => {
                ret.extend(
                    sorted
                        .into_iter()
                        .map(|x| x.1)
                        .chain(null_rows.into_iter())
                        .take(self.count),
                );
            }
        }
        self.rows = Some(ret.into());
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{
        expression::ColumnExpression, Column, DataSetCreator, Schema, Value, ValueType, transformation::Transformation,
    };

    use super::{NullPos, SortOrder, TopTransformation};

    #[tokio::test]
    async fn test_top() {
        let dataset = DataSetCreator::eager(
            Schema::from(vec![
                Column::new("a".to_string(), ValueType::Int),
                Column::new("b".to_string(), ValueType::Int),
            ]),
            vec![
                vec![Value::Int(1), Value::Int(2)],
                vec![Value::Int(2), Value::Int(1)],
                vec![Value::Int(3), Value::Int(3)],
                vec![Value::Int(4), Value::Int(4)],
                vec![Value::Int(5), Value::Int(5)],
                vec![Value::Int(6), Value::Int(6)],
                vec![Value::Int(7), Value::Int(7)],
                vec![Value::Int(8), Value::Int(8)],
                vec![Value::Int(9), Value::Null],
                vec![Value::Int(10), Value::Int(10)],
            ],
        );

        let transform = TopTransformation::new(
            5,
            Box::new(ColumnExpression {
                column_index: 1,
                column_name: "b".to_string(),
            }),
            Some(SortOrder::Ascending),
            Some(NullPos::First),
        );
        let (_, rows) = transform.transform(dataset).unwrap().eval().await;
        let rows = rows.into_iter().collect::<Vec<_>>();
        println!("{:?}", rows);
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][0], Value::Int(9));
        assert_eq!(rows[0][1], Value::Null);
        assert_eq!(rows[1][0], Value::Int(2));
        assert_eq!(rows[1][1], Value::Int(1));
        assert_eq!(rows[2][0], Value::Int(1));
        assert_eq!(rows[2][1], Value::Int(2));
        assert_eq!(rows[3][0], Value::Int(3));
        assert_eq!(rows[3][1], Value::Int(3));
        assert_eq!(rows[4][0], Value::Int(4));
        assert_eq!(rows[4][1], Value::Int(4));
    }
}
