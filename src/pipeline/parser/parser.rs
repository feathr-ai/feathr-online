use std::collections::HashMap;

use crate::pipeline::transformation::{NullPos, SortOrder};

use super::super::{Pipeline, PiperError};

peg::parser! {
    grammar pipeline_parser() for str {
        use super::super::super::*;
        use super::super::expression_builders::*;
        use super::super::operator_builder::*;
        use super::super::transformation_builder::*;
        use super::super::pipeline_builder::*;

        pub rule program() -> Vec<PipelineBuilder>
            = ps:( _ p:pipeline(){ p } )* _ { ps }

        pub rule pipeline() -> PipelineBuilder
            = _ name:identifier() _ "(" _ schema:schema() _ ")" _ t:transformation_list() _ ";" {
                PipelineBuilder {
                    name: name.to_string(),
                    input_schema: schema,
                    transformations: t,
                }
            }
        rule transformation_list() -> Vec<Box<dyn TransformationBuilder>>
            = t:(stage())* {
                t
            }

        rule stage() -> Box<dyn TransformationBuilder>
            = "|" _ t:transformation() _ {
                t
            }

        pub rule schema() -> Schema
            = fields:(schema_field() **<1,> list_sep()){
                Schema{columns: fields}
            }

        rule schema_field() -> Column
            = name:identifier() _ value_type:("as" _ vt:value_type() {vt})? {
                Column {
                    name: name.to_string(),
                    column_type: value_type.unwrap_or(ValueType::Dynamic),
                }
            }

        rule value_type() -> ValueType
            = t:$("bool" / "int" / "long" / "float" / "double" / "string" / "array" / "object") {
                match t {
                    "bool" => ValueType::Bool,
                    "int" => ValueType::Int,
                    "long" => ValueType::Long,
                    "float" => ValueType::Float,
                    "double" => ValueType::Double,
                    "string" => ValueType::String,
                    "array" => ValueType::Array,
                    "object" => ValueType::Object,
                    _ => unreachable!(),
                }
            }

        pub rule transformation() -> Box<dyn TransformationBuilder>
            = t:(take_transformation()
                / where_transformation()
                / project_transformation()
                / project_rename_transformation()
                / project_remove_transformation()
                / explode_transformation()
                / lookup_transformation()
                / top_transformation()
                / ignore_error_transformation()
            ) {t}

        pub rule ignore_error_transformation() -> Box<dyn TransformationBuilder>
            = "ignore-error" { IgnoreErrorTransformationBuilder::new() }
        pub rule take_transformation() -> Box<dyn TransformationBuilder>
            = "take" _ count:u64_lit() { TakeTransformationBuilder::new(count.get_long().unwrap() as usize) }
        pub rule where_transformation() -> Box<dyn TransformationBuilder>
            = "where" _ condition:expression() { WhereTransformationBuilder::new(condition) }
        pub rule project_transformation() -> Box<dyn TransformationBuilder>
            = "project" _ columns:(project_column_def() **<1,> list_sep()) {
                ProjectTransformationBuilder::new(columns)
            }
        pub rule project_rename_transformation() -> Box<dyn TransformationBuilder>
            = "project-rename" _ columns:(project_rename_column() **<1,> list_sep()) {
                ProjectRenameTransformationBuilder::new(columns)
            }
        pub rule project_remove_transformation() -> Box<dyn TransformationBuilder>
            = "project-remove" _ columns:(identifier() **<1,> list_sep()) {
                ProjectRemoveTransformationBuilder::new(columns)
            }
        pub rule explode_transformation() ->  Box<dyn TransformationBuilder>
            = ("explode" / "mv-expand") _ column:identifier() _ exploded_type:("as" _ vt:value_type() {vt})? {
                ExplodeTransformationBuilder::new(column.to_string(), exploded_type)
            }
        pub rule lookup_transformation() ->  Box<dyn TransformationBuilder>
            = "lookup" _ columns:(rename_with_type() **<1,> list_sep()) _ "from" _ source:identifier() _ "on" _ key:expression() {
                LookupTransformationBuilder::new(columns, source, key)
            }
        rule top_transformation() -> Box<dyn TransformationBuilder>
            = "top" _ count:u64_lit() _ "by" _ exp:expression()  _ order:sort_order()? _ null:null_pos()? {
                TopTransformationBuilder::new(count.get_long().unwrap() as usize, exp, order, null)
            }
        rule sort_order() -> SortOrder = "asc"{ SortOrder::Ascending} / "desc" {SortOrder::Descending}
        rule null_pos() -> NullPos = ("nulls" _ "first") {NullPos::First} / ("nulls" _ "last") {NullPos::Last}

        rule project_column_def() -> (String, Box<dyn ExpressionBuilder>)
            = name:identifier() _ "=" _ def:expression() {
                (name.to_string(), def)
            }
        rule project_rename_column() -> (String, String)
            = new:identifier() _ "=" _ old:identifier() {
                (old.to_string(), new.to_string())
            }

        rule rename_with_type() -> (String, Option<String>, ValueType)
            = (new_name:identifier() _ "=" _ name:identifier() _ "as" _ vt:value_type() { (name.to_string(), Some(new_name), vt) })
            / (new_name:identifier() _ "=" _ name:identifier() { (name.to_string(), Some(new_name), ValueType::Dynamic) })
            / (name:identifier() _ "as" _ vt:value_type() { (name.to_string(), None, vt) })
            / (name:identifier() { (name.to_string(), None, ValueType::Dynamic) })

        #[cache_left_rec]
        pub rule expression() -> Box<dyn ExpressionBuilder> = precedence!{
            x:(@) _ ">" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new(">")), vec![x, y])) }
            x:(@) _ "<" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("<")), vec![x, y])) }
            x:(@) _ ">=" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new(">=")), vec![x, y])) }
            x:(@) _ "<=" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("<=")), vec![x, y])) }
            x:(@) _ "==" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("==")), vec![x, y])) }
            x:(@) _ "!=" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("!=")), vec![x, y])) }
            --
            x:(@) _ "+" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("+")), vec![x, y])) }
            x:(@) _ "-" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("-")), vec![x, y])) }
            x:(@) _ "or" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("or")), vec![x, y])) }
            --
            x:(@) _ "*" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("*")), vec![x, y])) }
            x:(@) _ "/" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("/")), vec![x, y])) }
            x:(@) _ "and" _ y:@ { (OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("and")), vec![x, y])) }
            --
            "+" _ x:(@) { (OperatorExpressionBuilder::new((UnaryOperatorBuilder::new("+")), vec![x])) }
            "-" _ x:(@) { (OperatorExpressionBuilder::new((UnaryOperatorBuilder::new("-")), vec![x])) }
            "not" _ x:(@) { (OperatorExpressionBuilder::new((UnaryOperatorBuilder::new("not")), vec![x])) }
            --
            x:(@) _ "is" _ "null" { (OperatorExpressionBuilder::new((UnaryOperatorBuilder::new("is null")), vec![x])) }
            x:(@) _ "is" _ "not" _ "null" { (OperatorExpressionBuilder::new((UnaryOperatorBuilder::new("is not null")), vec![x])) }
            --
            f:function_call() _ idx:(index() ** _) {
                idx.into_iter().fold(f, |e, i| {
                    OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("index")), vec![e, i])
                })
            }
            c:dot_member_term() _ idx:(index() ** _) {
                idx.into_iter().fold(c, |e, i| {
                    OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("index")), vec![e, i])
                })
            }
            --
            lit:literal() {
                (LiteralExpressionBuilder::new(lit))
            }
            "(" _ e:expression() _ ")" _ idx:(index() ** _) {
                idx.into_iter().fold(e, |e, i| {
                    OperatorExpressionBuilder::new((BinaryOperatorBuilder::new("index")), vec![e, i])
                })
            }
        }

        rule index() -> Box<dyn ExpressionBuilder>
            = "[" _ idx:expression() _ "]" { idx }

        rule expression_list() -> Vec<Box<dyn ExpressionBuilder>> = e:expression() ** list_sep() { e }

        rule dot_member_term() -> Box<dyn ExpressionBuilder> = seg:dot_member() {
            let col = ColumnExpressionBuilder::new(seg[0].to_string());
            if seg.len()>1 {
                seg.into_iter().skip(1).fold(col, |acc, id| {
                    OperatorExpressionBuilder::new(
                        BinaryOperatorBuilder::new("dot"),
                        vec![acc, LiteralExpressionBuilder::new(id.to_string())],
                    )
                })
            } else {
                col
            }
        }

        rule expression_term() -> Box<dyn ExpressionBuilder> = "(" _ e:expression() _ ")" {
            e
        }

        rule function_call() -> Box<dyn ExpressionBuilder> = id:identifier() _ "(" _ args:expression_list() _ ")" {
            OperatorExpressionBuilder::new(FunctionOperatorBuilder::new(id), args)
        }

        /// `some_id` or `a.b.c`
        rule dot_member() -> Vec<String> = id:identifier() **<1,> (_ "." _) { id }

        rule list_sep() =  _ "," _

        rule identifier() -> String
            = s:$(!reserved_words() ['a'..='z' | 'A'..='Z']['a'..='z' | 'A'..='Z' | '0'..='9' | '_' ]*) { s.to_string() }

        rule literal() -> Value
            = v:(f64_lit() / u64_lit() / bool_lit() / string_lit() / constant_lit()) { v }

        rule null_lit() -> Value
            = "null" { Value::Null }

        rule f64_lit() -> Value
            = n:$(['0'..='9']+ "." ['0'..='9']*) {?
                n.parse().or(Err("f64")).map(|v: f64| v.into())
            }

        rule u64_lit() -> Value
            = n:$(['0'..='9']+) {?
                n.parse().or(Err("u64")).map(|v: u64| v.into())
            }

        rule constant_lit() -> Value
            = pi() / e() / tau()

        rule pi() -> Value
            = "PI" { Value::Double(std::f64::consts::PI) }

        rule e() -> Value
            = "E" { Value::Double(std::f64::consts::E) }

        rule tau() -> Value
            = "TAU" { Value::Double(std::f64::consts::TAU) }

        rule bool_lit() -> Value
            = v:$("true" / "false") {? v.parse().or(Err("bool")).map(|v: bool| v.into()) }

        rule string_lit() -> Value
            = "\"" chars:(char()*) "\"" { return chars.join("").into() }

        rule char() -> String
            = c:(unescaped() / escape_sequence()) { return c.to_string() }

        rule escape_sequence() -> String
            = "\\" c:$("\"" / "\\" / "n" / "r" / "t") { return match c {
                "\"" => "\"",
                "\\" => "\\",
                "r" => "\r",
                "n" => "\n",
                "t" => "\t",
                _ => unreachable!("Shouldn't reach here")
            }.to_string() }

        rule unescaped() -> String
            = c:$([^ '\0'..='\x1F' | '\x22' |'\x5C']) { return c.to_string() }

        rule DIGIT() -> String
            = c:$(['0'..='9']) { return c.to_string() }
        rule HEXDIG() -> String
            = c:$(['0'..='9' | 'a'..='f' | 'A'..='F']) { return c.to_string() }

        rule reserved_words()
            = "null" / "true" / "false" / "and" / "or" / "not" / "is" / "as" / "int" / "long" / "float" / "double" / "array" / "object" / "dynamic" / "PI" / "E" / "TAU"

        rule _() = quiet!{ (whitespace_char() / "\n" / comment())* }
        rule whitespace_char() = ['\t' | ' ']
        rule comment() = "#" (!"\n" [_])* ("\n" / ![_])
    }
}

pub fn parse_script(input: &str) -> Result<HashMap<String, Pipeline>, PiperError> {
    let pipelines = pipeline_parser::program(input)
        .map_err(|e| PiperError::SyntaxError(e.to_string()))?
        .into_iter()
        .map(|p| p.build())
        .collect::<Result<Vec<_>, _>>()?;
    Ok(pipelines.into_iter().map(|p| (p.name.clone(), p)).collect())
}

pub fn parse_pipeline(input: &str) -> Result<Pipeline, PiperError> {
    pipeline_parser::pipeline(input)
        .map_err(|e| PiperError::SyntaxError(e.to_string()))?
        .build()
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{Column, Schema, ValueType};

    use super::pipeline_parser;

    #[test]
    fn test_parse_comments() {
        let input = "1 #2222
        #dasdfasdf
        +1";
        let result = pipeline_parser::expression(input);
        assert!(result.is_ok());
        let schema = Schema::new();
        let expr = result.unwrap().build(&schema);
        println!("{}", expr.unwrap().dump());
    }

    #[test]
    fn test_parse1() {
        let input = "a + b.x.y + f.a.b.c[12] + \"ddd\\t\"";
        let result = pipeline_parser::expression(input);
        assert!(result.is_ok());
        let schema: Schema = [
            Column::new("a", ValueType::Long),
            Column::new("b", ValueType::Object),
            Column::new("f", ValueType::Array),
        ]
        .into_iter()
        .collect();
        let expr = result.unwrap().build(&schema);
        println!("{}", expr.unwrap().dump());
    }

    #[test]
    fn test_array_index() {
        let input = "(f(12)+a[2] + x.y.z[78] -b)[12] [34][56]";
        let result = pipeline_parser::expression(input);
        println!("{:?}", result);
        assert!(result.is_ok());
    }
}
