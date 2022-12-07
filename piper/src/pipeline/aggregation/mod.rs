use std::fmt::Debug;
use std::{collections::HashMap, sync::Arc};

use dyn_clonable::clonable;

use crate::{PiperError, Value, ValueType};

use super::expression::Expression;

mod count;
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
        Box::new(count::Count::default()) as Box<dyn AggregationFunction>,
    );
    agg.insert(
        "sum".to_string(),
        Box::new(sum::Sum::default()) as Box<dyn AggregationFunction>,
    );
    agg
}
