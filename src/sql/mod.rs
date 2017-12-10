//! This is the parser mod, implemented with nom.
//! #parse_sql will be the entrance and
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
}
