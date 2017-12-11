//! This is the parser mod, implemented with nom.
//! #parse will be the entrance and
//! ParsedSQL will be the final result.

use nom::{alphanumeric, IResult};
use std::str;
pub mod operands;
use self::operands::{parse_operand, Operand};

pub type TableName = String;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum SQLType {
    Integer,
    // Float,
    // Boolean,
    String,
    // Text,
    // DateTime
}

#[derive(Debug, Eq, PartialEq)]
pub enum ParsedSQL {
    Select {
        table: Option<TableName>,
        operands: Vec<Operand>,
    },
}

named!(parse_multiple_operands(&[u8]) -> Vec<Operand>,
    alt!(
        map!(ws!(tag!("*")), |_| Vec::new()) |
        separated_list_complete!(tag!(","), parse_operand)
    )
);

named!(parse_table_name(&[u8]) -> TableName,
    ws!(map_res!(alphanumeric, |bytes| str::from_utf8(bytes).map(|str| str.to_owned())))
);

named!(parse_sql(&[u8]) -> ParsedSQL,
    ws!(map!(
        tuple!(
            tag!("select"),
            parse_multiple_operands,
            opt!(complete!(preceded!(tag!("from"), parse_table_name)))
        ),
        |(_, op, table)| ParsedSQL::Select {operands: op, table: table}
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
            table: None,
            operands: vec![Operand::Integer(42)],
        };
        assert_eq!(parse_sql(b"select 42"), IResult::Done(EMPTY, expected));
    }

    #[test]
    fn can_recognize_a_select_text_statement() {
        let expected = ParsedSQL::Select {
            table: None,
            operands: vec![Operand::String("nihao, rdb.".to_owned())],
        };
        assert_eq!(
            parse_sql(b"select 'nihao, rdb.'"),
            IResult::Done(EMPTY, expected)
        );
    }

    #[test]
    fn can_recognize_a_select_statement_for_multiple_columns() {
        let expected = ParsedSQL::Select {
            table: None,
            operands: vec![
                Operand::String("nihao, rdb.".to_owned()),
                Operand::Integer(42),
                Operand::String("e".to_owned()),
            ],
        };
        assert_eq!(
            parse_sql(b"select 'nihao, rdb.', 42, 'e'"),
            IResult::Done(EMPTY, expected)
        );
    }

    #[test]
    fn can_recognize_the_select_all_from_table_statement() {
        let expected = ParsedSQL::Select {
            table: Some("users".to_owned()),
            operands: Vec::new(),
        };

        assert_eq!(
            parse_sql(b"select * from users"),
            IResult::Done(EMPTY, expected)
        );
    }

    #[test]
    fn can_recognize_the_select_columns_from_table_statement() {
        let expected = ParsedSQL::Select {
            table: Some("users".to_owned()),
            operands: vec![Operand::Column("id".to_owned()), Operand::Integer(42)],
        };

        assert_eq!(
            parse_sql(b"select id, 42 from users"),
            IResult::Done(EMPTY, expected)
        );
    }
}
