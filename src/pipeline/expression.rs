use std::fmt::Debug;

use dyn_clonable::clonable;

use super::{PiperError, Value, ValueType};

use super::operator::Operator;

#[clonable]
pub trait Expression: Clone + Debug + Send + Sync {
    fn get_output_type(&self, schema: &[ValueType]) -> Result<ValueType, PiperError>;

    fn eval(&self, row: &[Value]) -> Result<Value, PiperError>;

    fn dump(&self) -> String;
}

#[derive(Clone, Debug)]
pub struct ColumnExpression {
    pub column_name: String,
    pub column_index: usize,
}

impl Expression for ColumnExpression {
    fn get_output_type(&self, schema: &[ValueType]) -> Result<ValueType, PiperError> {
        if self.column_index >= schema.len() {
            // This shouldn't happen, because the index is set by ColumnExpressionBuilder at the parsing time
            panic!("Column index out of range");
        }
        Ok(schema[self.column_index])
    }

    fn eval(&self, row: &[Value]) -> Result<Value, PiperError> {
        if self.column_index >= row.len() {
            // This shouldn't happen, because the index is set by ColumnExpressionBuilder at the parsing time
            panic!("Column index out of range");
        }
        Ok(row[self.column_index].clone())
    }

    fn dump(&self) -> String {
        self.column_name.to_owned()
    }
}
#[derive(Clone, Debug)]
pub struct LiteralExpression {
    pub value: Value,
}

impl Expression for LiteralExpression {
    fn get_output_type(&self, _schema: &[ValueType]) -> Result<ValueType, PiperError> {
        Ok(self.value.value_type())
    }

    fn eval(&self, _row: &[Value]) -> Result<Value, PiperError> {
        Ok(self.value.clone())
    }

    fn dump(&self) -> String {
        self.value.dump()
    }
}

#[derive(Clone, Debug)]
pub struct OperatorExpression {
    pub operator: Box<dyn Operator>,
    pub arguments: Vec<Box<dyn Expression>>,
}

impl Expression for OperatorExpression {
    fn get_output_type(&self, schema: &[ValueType]) -> Result<ValueType, PiperError> {
        self.operator.get_output_type(&self.arguments.iter().map(|arg| arg.get_output_type(schema)).collect::<Result<Vec<ValueType>, PiperError>>()?)
    }

    fn eval(&self, row: &[Value]) -> Result<Value, PiperError> {
        let arguments = self
            .arguments
            .iter()
            .map(|e| e.eval(row))
            .collect::<Result<Vec<_>, _>>()?;
        self.operator.eval(arguments)
    }

    fn dump(&self) -> String {
        self.operator
            .dump(self.arguments.iter().map(|e| e.dump()).collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{Value, operator::LessThanOperator, expression::Expression};

    use super::{ColumnExpression, LiteralExpression, OperatorExpression};

    #[test]
    fn test_operator() {
        let l = ColumnExpression {
            column_name: "a".to_owned(),
            column_index: 0,
        };
        let r = LiteralExpression {
            value: 42.into(),
        };
        let e = OperatorExpression {
            operator: Box::new(LessThanOperator {}),
            arguments: vec![Box::new(l), Box::new(r)],
        };
        let row: Vec<Value> = vec![100.into()];
        assert_eq!(e.eval(&row).unwrap(), false.into());
        let row: Vec<Value> = vec![21.into()];
        assert_eq!(e.eval(&row).unwrap(), true.into());
    }
}