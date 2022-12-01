use std::collections::HashMap;

use crate::pipeline::{
    pipelines::BuildContext,
    transformation::{NullPos, SortOrder},
};

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
                / project_keep_transformation()
                / explode_transformation()
                / lookup_transformation()
                / top_transformation()
                / ignore_error_transformation()
            ) {t}

        pub rule ignore_error_transformation() -> Box<dyn TransformationBuilder>
            = "ignore-error" { IgnoreErrorTransformationBuilder::create() }
        pub rule take_transformation() -> Box<dyn TransformationBuilder>
            = "take" _ count:u64_lit() { TakeTransformationBuilder::create(count.get_long().unwrap() as usize) }
        pub rule where_transformation() -> Box<dyn TransformationBuilder>
            = "where" _ condition:expression() { WhereTransformationBuilder::create(condition) }
        pub rule project_transformation() -> Box<dyn TransformationBuilder>
            = "project" _ columns:(project_column_def() **<1,> list_sep()) {
                ProjectTransformationBuilder::create(columns)
            }
        pub rule project_rename_transformation() -> Box<dyn TransformationBuilder>
            = "project-rename" _ columns:(project_rename_column() **<1,> list_sep()) {
                ProjectRenameTransformationBuilder::create(columns)
            }
        pub rule project_remove_transformation() -> Box<dyn TransformationBuilder>
            = "project-remove" _ columns:(identifier() **<1,> list_sep()) {
                ProjectRemoveTransformationBuilder::create(columns)
            }
        pub rule project_keep_transformation() -> Box<dyn TransformationBuilder>
            = "project-keep" _ columns:(identifier() **<1,> list_sep()) {
                ProjectKeepTransformationBuilder::create(columns)
            }
        pub rule explode_transformation() ->  Box<dyn TransformationBuilder>
            = ("explode" / "mv-expand") _ column:identifier() _ exploded_type:("as" _ vt:value_type() {vt})? {
                ExplodeTransformationBuilder::create(column.to_string(), exploded_type)
            }
        pub rule lookup_transformation() ->  Box<dyn TransformationBuilder>
            = "lookup" _ columns:(rename_with_type() **<1,> list_sep()) _ "from" _ source:identifier() _ "on" _ key:expression() {
                LookupTransformationBuilder::new(columns, source, key)
            }
        rule top_transformation() -> Box<dyn TransformationBuilder>
            = "top" _ count:u64_lit() _ "by" _ exp:expression()  _ order:sort_order()? _ null:null_pos()? {
                TopTransformationBuilder::create(count.get_long().unwrap() as usize, exp, order, null)
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

        pub rule expression() -> Box<dyn ExpressionBuilder> = precedence!{
            x:(@) _ ">" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create(">")), vec![x, y])) }
            x:(@) _ "<" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("<")), vec![x, y])) }
            x:(@) _ ">=" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create(">=")), vec![x, y])) }
            x:(@) _ "<=" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("<=")), vec![x, y])) }
            x:(@) _ "==" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("==")), vec![x, y])) }
            x:(@) _ "!=" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("!=")), vec![x, y])) }
            x:(@) _ "<>" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("!=")), vec![x, y])) }
            --
            x:(@) _ "+" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("+")), vec![x, y])) }
            x:(@) _ "-" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("-")), vec![x, y])) }
            x:(@) _ "or" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("or")), vec![x, y])) }
            --
            x:(@) _ "*" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("*")), vec![x, y])) }
            x:(@) _ "/" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("/")), vec![x, y])) }
            x:(@) _ "div" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("div")), vec![x, y])) }
            x:(@) _ "%" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("%")), vec![x, y])) }
            x:(@) _ "&" _ y:@ { (OperatorExpressionBuilder::create((FunctionOperatorBuilder::create("bit_and")), vec![x, y])) }
            x:(@) _ "&&" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("and")), vec![x, y])) }
            x:(@) _ "and" _ y:@ { (OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("and")), vec![x, y])) }
            --
            "+" _ x:(@) { (OperatorExpressionBuilder::create((UnaryOperatorBuilder::create("+")), vec![x])) }
            "-" _ x:(@) { (OperatorExpressionBuilder::create((UnaryOperatorBuilder::create("-")), vec![x])) }
            "~" _ x:(@) { (OperatorExpressionBuilder::create((FunctionOperatorBuilder::create("bit_not")), vec![x])) }
            "!" _ x:(@) { (OperatorExpressionBuilder::create((UnaryOperatorBuilder::create("not")), vec![x])) }
            "not" _ x:(@) { (OperatorExpressionBuilder::create((UnaryOperatorBuilder::create("not")), vec![x])) }
            --
            x:(@) _ "is" _ "null" { (OperatorExpressionBuilder::create((UnaryOperatorBuilder::create("is null")), vec![x])) }
            x:(@) _ "is" _ "not" _ "null" { (OperatorExpressionBuilder::create((UnaryOperatorBuilder::create("is not null")), vec![x])) }
            --
            case:case_clause() { case }
            f:function_call() _ idx:(index() ** _) {
                idx.into_iter().fold(f, |e, i| {
                    OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("index")), vec![e, i])
                })
            }
            c:dot_member_term() _ idx:(index() ** _) {
                idx.into_iter().fold(c, |e, i| {
                    OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("index")), vec![e, i])
                })
            }
            --
            lit:literal() {
                (LiteralExpressionBuilder::create(lit))
            }
            "(" _ e:expression() _ ")" _ idx:(index() ** _) {
                idx.into_iter().fold(e, |e, i| {
                    OperatorExpressionBuilder::create((BinaryOperatorBuilder::create("index")), vec![e, i])
                })
            }
        }

        rule case_clause() -> Box<dyn ExpressionBuilder>
            = "case"
                _ when_then:(when_then() **<1,> _)
                _ else_then:else_then()?
                _ "end"
            {
                let args = when_then.into_iter().flat_map(|(w, t)| [w, t].into_iter()).chain(else_then.into_iter()).collect();
                OperatorExpressionBuilder::create(FunctionOperatorBuilder::create("case"), args)
            }

        rule when_then() -> (Box<dyn ExpressionBuilder>, Box<dyn ExpressionBuilder>)
            = "when" _ condition:expression() _ "then" _ result:expression() {
                (condition, result)
            }

        rule else_then() -> Box<dyn ExpressionBuilder>
            = "else" _ result:expression() {
                result
            }

        rule index() -> Box<dyn ExpressionBuilder>
            = "[" _ idx:expression() _ "]" { idx }

        rule expression_list() -> Vec<Box<dyn ExpressionBuilder>> = e:expression() ** list_sep() { e }

        rule dot_member_term() -> Box<dyn ExpressionBuilder> = seg:dot_member() {
            let col = ColumnExpressionBuilder::create(seg[0].to_string());
            if seg.len()>1 {
                seg.into_iter().skip(1).fold(col, |acc, id| {
                    OperatorExpressionBuilder::create(
                        BinaryOperatorBuilder::create("dot"),
                        vec![acc, LiteralExpressionBuilder::create(id)],
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
            OperatorExpressionBuilder::create(FunctionOperatorBuilder::create(id), args)
        }

        /// `some_id` or `a.b.c`
        rule dot_member() -> Vec<String> = id:identifier() **<1,> (_ "." _) { id }

        rule list_sep() =  _ "," _

        rule identifier() -> String
            = s:$(!reserved_words() ['a'..='z' | 'A'..='Z']['a'..='z' | 'A'..='Z' | '0'..='9' | '_' ]*) { s.to_string() }

        rule literal() -> Value
            = v:(f64_lit() / u64_lit() / bool_lit() / string_lit() / constant_lit() / null_lit()) { v }

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
            = "\"" chars:(char()*) "\"" { chars.join("").into() }

        rule char() -> String
            = c:(unescaped() / escape_sequence()) { c.to_string() }

        rule escape_sequence() -> String
            = "\\" c:$("\"" / "\\" / "n" / "r" / "t") { match c {
                "\"" => "\"",
                "\\" => "\\",
                "r" => "\r",
                "n" => "\n",
                "t" => "\t",
                _ => unreachable!("Shouldn't reach here")
            }.to_string() }

        rule unescaped() -> String
            = c:$([^ '\0'..='\x1F' | '\x22' |'\x5C']) { c.to_string() }

        rule DIGIT() -> String
            = c:$(['0'..='9']) { c.to_string() }
        rule HEXDIG() -> String
            = c:$(['0'..='9' | 'a'..='f' | 'A'..='F']) { c.to_string() }

        rule reserved_words()
            = "null" / "true" / "false" / "and" / "or" / "not" / "is" / "as" / "dynamic" / "PI" / "E" / "TAU" / "case" / "when" / "then" / "else"

        rule _() = quiet!{ (whitespace_char() / "\n" / comment())* }
        rule whitespace_char() = ['\t' | ' ']
        rule comment() = "#" (!"\n" [_])* ("\n" / ![_])
    }
}

pub fn parse_script(
    input: &str,
    ctx: &BuildContext,
) -> Result<HashMap<String, Pipeline>, PiperError> {
    let pipelines = pipeline_parser::program(input)
        .map_err(|e| PiperError::SyntaxError(e.to_string()))?
        .into_iter()
        .map(|p| p.build(ctx))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(pipelines.into_iter().map(|p| (p.name.clone(), p)).collect())
}

pub fn parse_pipeline(input: &str, ctx: &BuildContext) -> Result<Pipeline, PiperError> {
    pipeline_parser::pipeline(input)
        .map_err(|e| PiperError::SyntaxError(e.to_string()))?
        .build(ctx)
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{
        pipelines::BuildContext, Column, Schema, ValueType,
    };

    use super::pipeline_parser;

    #[test]
    fn test_parse_comments() {
        let input = "1 #2222
        #dasdfasdf
        +1";
        let result = pipeline_parser::expression(input);
        assert!(result.is_ok());
        let schema = Schema::default();
        let expr = result.unwrap().build(&schema, &BuildContext::default());
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
        let expr = result.unwrap().build(&schema, &BuildContext::default());
        println!("{}", expr.unwrap().dump());
    }

    #[test]
    fn test_operators() {
        let input = "1+2-3*4/5%6 div ~7 & !8 or (9 && not 10 and 11) & ~11 > 1 < 2 >=3 <=4 != 5 == case when 1 then 2 when 3 then 4 else 5 end <> null";
        let result = pipeline_parser::expression(input);
        assert!(result.is_ok());
        println!(
            "{:?}",
            result
                .unwrap()
                .build(&Schema::default(), &BuildContext::default())
                .unwrap()
                .dump()
        );
    }

    #[test]
    fn test_array_index() {
        let input = "(f(12)+a[2] + x.y.z[78] -b)[12] [34][56]";
        let result = pipeline_parser::expression(input);
        println!("{:?}", result);
        assert!(result.is_ok());
    }

    #[test]
    fn test_case_clause() {
        let input = "case when (a > 1) then (2) when a>2 then 2 else 4 end";
        let result = pipeline_parser::expression(input);
        println!("{:?}", result);
        assert!(result.is_ok());
    }
}
