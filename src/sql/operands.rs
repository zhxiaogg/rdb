//! operands are lowest elements of a statement, including:
//! - primitive values (basic operand)
//! - parentheses surrounded operands (basic operand)
//! - arithmetic expressions
//! - bool expressions
//! - columns (basic operand)

use std::str::{FromStr, from_utf8};
use nom::{digit, IResult};

#[derive(Debug, PartialEq, Eq)]
pub enum Operand {
    /// primitive of integer type, size of 64 bits
    Integer(i64),

    Parentheses(Box<Operand>),

    Add(Box<Operand>, Box<Operand>),

    String(String),
    // Alias(Operand, String)
}

named!(_parse_i64( &[u8] ) -> i64, ws!(map_res!(map_res!(digit, from_utf8),FromStr::from_str)));

named!(_parse_signed_i64( &[u8] ) -> i64,
    ws!(map!(
        pair!(alt!(tag!("+") | tag!("-") | value!(&b"+"[..])), _parse_i64),
        |(sign, value)| match sign {
            s if s == &b"-"[..] => -value,
            _ => value
        }
    ))
);

named!(parse_integer_operand(&[u8]) -> Operand,
    map!(_parse_signed_i64, |v| Operand::Integer(v)));

named!(parse_parens_operand(&[u8]) -> Operand,
    ws!(map!(
        tuple!(tag!("("), parse_operand, tag!(")")),
        |(_, op, _)| Operand::Parentheses(Box::new(op))
    ))
);

named!(parse_basic_operand(&[u8]) -> Operand,
    alt!(parse_integer_operand | parse_parens_operand)
);

named!(parse_str_operand(&[u8]) -> Operand,
    ws!(map_res!(
        delimited!(tag!("'"), is_not!("'"), tag!("'")),
        |bytes| from_utf8(bytes).map(|str| Operand::String(str.to_owned()))
    ))
);

named!(parse_add_operand(&[u8]) -> Operand,
    map!(tuple!(parse_basic_operand, ws!(tag!("+")), parse_basic_operand),
        |(v1, _, v2)| Operand::Add(Box::new(v1), Box::new(v2))
    )
);

named!(pub parse_operand(&[u8]) -> Operand,
    alt_complete!(parse_add_operand | parse_basic_operand | parse_str_operand)
);

#[cfg(test)]
mod test {
    use super::*;
    const EMPTY: &[u8] = &[0u8; 0];

    #[test]
    fn can_parse_integer() {
        assert_eq!(_parse_i64(b"42"), IResult::Done(EMPTY, 42));
        assert_eq!(_parse_i64(b" 42"), IResult::Done(EMPTY, 42));
    }

    #[test]
    fn can_parse_signed_integer() {
        assert_eq!(_parse_signed_i64(b"+42"), IResult::Done(EMPTY, 42));
        assert_eq!(_parse_signed_i64(b"-42"), IResult::Done(EMPTY, -42));
        assert_eq!(_parse_signed_i64(b" - 42 "), IResult::Done(EMPTY, -42));
    }

    #[test]
    fn can_recognize_a_integer_operand() {
        assert_eq!(
            parse_integer_operand(b"-42"),
            IResult::Done(EMPTY, Operand::Integer(-42))
        );
    }

    #[test]
    fn can_recognize_a_add_operand() {
        let expected = Operand::Add(
            Box::new(Operand::Integer(42)),
            Box::new(Operand::Integer(43)),
        );
        assert_eq!(
            parse_add_operand(b" 42 + 43 "),
            IResult::Done(EMPTY, expected)
        );
    }

    #[test]
    fn can_recognize_a_parens_operand() {
        let expected = Operand::Parentheses(Box::new(Operand::Integer(42)));
        assert_eq!(
            parse_parens_operand(b" ( 42 ) "),
            IResult::Done(EMPTY, expected)
        );
    }

    #[test]
    fn can_parse_basic_operands() {
        assert_eq!(
            parse_basic_operand(b" -42 "),
            IResult::Done(EMPTY, Operand::Integer(-42))
        );

        let expected = Operand::Parentheses(Box::new(Operand::Integer(-42)));
        assert_eq!(
            parse_basic_operand(b" (-42 ) "),
            IResult::Done(EMPTY, expected)
        );
    }

    #[test]
    fn can_recognize_a_string_literal() {
        let expected = Operand::String(" as df ".to_owned());
        assert_eq!(
            parse_str_operand(b" ' as df ' "),
            IResult::Done(EMPTY, expected)
        );
    }

    #[test]
    fn can_parse_any_operands_in_this_universe() {
        assert_eq!(
            parse_operand(b" -42 "),
            IResult::Done(EMPTY, Operand::Integer(-42))
        );

        let mut expected = Operand::Add(
            Box::new(Operand::Integer(-42)),
            Box::new(Operand::Integer(5)),
        );
        assert_eq!(parse_operand(b"-42 + 5"), IResult::Done(EMPTY, expected));

        let add_ops = Operand::Add(Box::new(Operand::Integer(5)), Box::new(Operand::Integer(3)));
        let parens_ops = Operand::Parentheses(Box::new(add_ops));
        expected = Operand::Add(Box::new(Operand::Integer(-42)), Box::new(parens_ops));
        assert_eq!(
            parse_operand(b"-42 + (5 + 3)"),
            IResult::Done(EMPTY, expected)
        );

        expected = Operand::String("nihao.".to_owned());
        assert_eq!(parse_operand(b"'nihao.'"), IResult::Done(EMPTY, expected))
    }
}
