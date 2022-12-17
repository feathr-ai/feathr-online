use std::fmt::Debug;
use std::{collections::HashMap, sync::Arc};

use dyn_clonable::clonable;

use crate::{PiperError, Value, ValueType};

use super::expression::Expression;

mod all_any;
mod array_agg;
mod count;
mod first_last;
mod min_max;
mod sum;

#[clonable]
pub trait AggregationFunction: Send + Sync + Clone + Debug {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError>;
    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError>;
    fn get_result(&self) -> Result<Value, PiperError>;
    fn dump(&self) -> String;
}

#[derive(Debug, Clone)]
pub struct Aggregation {
    pub aggregation: Box<dyn AggregationFunction>,
    pub arguments: Arc<Vec<Box<dyn Expression>>>,
}

impl Aggregation {
    pub fn new(
        aggregation: Box<dyn AggregationFunction>,
        arguments: Vec<Box<dyn Expression>>,
    ) -> Self {
        Self {
            aggregation,
            arguments: Arc::new(arguments),
        }
    }

    pub fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        let arguments = self
            .arguments
            .iter()
            .map(|e| e.get_output_type(input_type))
            .collect::<Result<Vec<_>, _>>()?;
        self.aggregation.get_output_type(&arguments)
    }

    pub fn feed(&mut self, row: &[Value]) -> Result<(), PiperError> {
        let arguments = self
            .arguments
            .iter()
            .map(|e| e.eval(row))
            .collect::<Vec<_>>();
        self.aggregation.feed(&arguments)
    }

    pub fn get_result(&self) -> Result<Value, PiperError> {
        self.aggregation.get_result()
    }

    pub fn dump(&self) -> String {
        format!(
            "{}({})",
            self.aggregation.dump(),
            self.arguments
                .iter()
                .map(|e| e.dump())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

pub fn init_built_in_agg_functions() -> HashMap<String, Box<dyn AggregationFunction>> {
    let mut agg = HashMap::new();
    agg.insert(
        "count".to_string(),
        Box::<count::Count>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "count_if".to_string(),
        Box::<count::CountIf>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "distinct_count".to_string(),
        Box::<count::DistinctCount>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "sum".to_string(),
        Box::<sum::Sum>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "avg".to_string(),
        Box::<sum::Avg>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "mean".to_string(),
        Box::<sum::Avg>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "min".to_string(),
        Box::<min_max::Min>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "max".to_string(),
        Box::<min_max::Max>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "least".to_string(),
        Box::<min_max::Min>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "greatest".to_string(),
        Box::<min_max::Max>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "min_by".to_string(),
        Box::<min_max::MinBy>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "max_by".to_string(),
        Box::<min_max::MaxBy>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "every".to_string(),
        Box::<all_any::All>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "any".to_string(),
        Box::<all_any::Any>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "some".to_string(),
        Box::<all_any::Any>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "first".to_string(),
        Box::<first_last::First>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "last".to_string(),
        Box::<first_last::Last>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "first_value".to_string(),
        Box::<first_last::First>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "last_value".to_string(),
        Box::<first_last::Last>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "array_agg".to_string(),
        Box::<array_agg::ArrayAgg>::default() as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "array_agg_if".to_string(),
        Box::<array_agg::ArrayAggIf>::default() as Box<dyn AggregationFunction>,
    );
    agg
}
