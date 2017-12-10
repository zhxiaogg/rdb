//! This is the parser mod, implemented with nom.
//! #parse function will be the entrance and
//! ParsedStatement will be the final result.

use std::str::{FromStr, from_utf8};
use nom::{digit, IResult};

pub enum ParsedStatement {
    Select { operands: Vec<Operand> },
}

/// operands are lowest elements of a statement, including:
/// - primitive values
/// - arithmetic expressions,
/// - bool expressions
/// - columns
#[derive(Debug, PartialEq, Eq)]
pub enum Operand {
    /// primitive of integer type, size of 64 bits
    Integer(i64),
    // Add(Operand, Operand),

  // Alias(Operand, String)
}

named!(_parse_i64( &[u8] ) -> i64, map_res!(map_res!(digit, from_utf8),FromStr::from_str));
named!(parse_integer_operand( &[u8] ) -> Operand,
    map_res!(_parse_i64, |v| {Result::Ok::<Operand,String>(Operand::Integer(v))}));

#[cfg(test)]
mod test {
    use super::*;
    
    #[test]
    fn can_parse_integer() {
        assert_eq!(_parse_i64(b"42"), IResult::Done(&b""[..], 42));
    }

    #[test]
    fn parser_can_recognize_a_integer_oprand() {
        assert_eq!(
            parse_integer_operand(b"42"),
            IResult::Done(&b""[..], Operand::Integer(42))
        );
    }
}
