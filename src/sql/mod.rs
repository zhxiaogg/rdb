//! This is the parser mod, implemented with nom.
//! #parse will be the entrance and
//! ParsedSQL will be the final result.

use nom::IResult;

pub mod operands;
use self::operands::{parse_operand, Operand};


#[derive(Debug, Eq, PartialEq)]
pub enum ParsedSQL {
    Select { operands: Vec<Operand> },
}

named!(parse_sql(&[u8]) -> ParsedSQL,
    ws!(map!(
        pair!(tag!("select"), parse_operand),
        |(_, op)| ParsedSQL::Select {operands: vec![op]}
    ))
);

pub fn parse(inputs: &[u8]) -> Result<ParsedSQL, String> {
    parse_sql(inputs)
        .to_result()
        .map_err(|_| "parse failed.".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY: &[u8] = &[0u8; 0];

    #[test]
    fn can_recognize_simplest_select_statement() {
        let expected = ParsedSQL::Select {
            operands: vec![Operand::Integer(42)],
        };
        assert_eq!(parse_sql(b"select 42"), IResult::Done(EMPTY, expected));
    }

    #[test]
    fn can_recognize_a_select_text_statement() {
        let expected = ParsedSQL::Select {
            operands: vec![Operand::String("nihao, rdb.".to_owned())],
        };
        assert_eq!(
            parse_sql(b"select 'nihao, rdb.'"),
            IResult::Done(EMPTY, expected)
        );
    }
}
