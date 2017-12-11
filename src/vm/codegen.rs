use std::vec::Vec;

use sql::{ParsedSQL, SQLType};
use sql::operands::Operand;
use table::schema::Schema;

pub type ErrCode = u32;

#[derive(Debug, Eq, PartialEq)]
pub enum OpCode {
    /// load a constant integer value into stack
    LoadInt(i64),
    LoadStr(String),
    /// store integer value in stack to result row buffer
    StoreInt,
    StoreStr,
    Add,
    FlushRow,
    Exit(ErrCode),
}

/// size in bytes for SQLTypes
pub fn size_of(sql_type: SQLType) -> usize {
    match sql_type {
        SQLType::Integer => 8,
        SQLType::String => 0,
    }
}

pub fn gen_code(sql: &ParsedSQL, schema: &Schema) -> Vec<OpCode> {
    let mut op_codes: Vec<OpCode> = Vec::new();
    match sql {
        &ParsedSQL::Select {
            ref table,
            ref operands,
        } => {
            // code for all columns
            for op in operands {
                translate_operand_to_code(&mut op_codes, &op);

                let store_code = store_code_for_type(type_of(&op, schema).unwrap());
                op_codes.push(store_code);
            }

            // flush row when all operands' codes finished
            op_codes.push(OpCode::FlushRow);
        }
    };

    op_codes
}

fn store_code_for_type(sql_type: SQLType) -> OpCode {
    match sql_type {
        SQLType::Integer => OpCode::StoreInt,
        SQLType::String => OpCode::StoreStr,
        // _ => OpCode::Exit(1),
    }
}

/// type inference for the operand
fn type_of(op: &Operand, schema: &Schema) -> Option<SQLType> {
    match op {
        &Operand::Integer(_) => Some(SQLType::Integer),
        &Operand::Add(ref op1, ref op2) => {
            let type_op1 = type_of(op1, schema);
            if type_op1 == type_of(op2, schema) {
                type_op1
            } else {
                // TODO: cast
                None
            }
        }
        &Operand::Parentheses(ref op) => type_of(op, schema),
        &Operand::String(ref str) => Some(SQLType::String),
        &Operand::Column(ref column) => schema.get_column_type(column),
    }
}

fn translate_operand_to_code(op_codes: &mut Vec<OpCode>, op: &Operand) {
    match op {
        &Operand::Integer(v) => op_codes.push(OpCode::LoadInt(v)),
        &Operand::Add(ref op1, ref op2) => {
            translate_operand_to_code(op_codes, op1);
            translate_operand_to_code(op_codes, op2);
            op_codes.push(OpCode::Add)
        }
        &Operand::Parentheses(ref op) => {
            translate_operand_to_code(op_codes, op);
        }
        &Operand::String(ref str) => op_codes.push(OpCode::LoadStr(str.to_owned())),
        &Operand::Column(ref column) => panic!("not implemented"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_schema() -> Schema {
        Schema::new()
    }

    #[test]
    fn gen_codes_for_a_single_load() {
        let mut op_codes = Vec::new();
        let op = Operand::Integer(42);
        translate_operand_to_code(&mut op_codes, &op);

        let expected = vec![OpCode::LoadInt(42)];
        assert_eq!(op_codes, expected);
    }

    #[test]
    fn gen_codes_for_add_ops() {
        let mut op_codes = Vec::new();
        // 3 + (4 + 5)
        let add_op = Operand::Add(Box::new(Operand::Integer(4)), Box::new(Operand::Integer(5)));
        let nested_add_op = Operand::Add(Box::new(Operand::Integer(3)), Box::new(add_op));
        translate_operand_to_code(&mut op_codes, &nested_add_op);

        let expected = vec![
            OpCode::LoadInt(3),
            OpCode::LoadInt(4),
            OpCode::LoadInt(5),
            OpCode::Add,
            OpCode::Add,
        ];
        assert_eq!(op_codes, expected);
    }

    #[test]
    fn type_inference_for_constants_done_right() {
        let schema = get_schema();
        // 3 + (4 + 5)
        let add_op = Operand::Add(Box::new(Operand::Integer(4)), Box::new(Operand::Integer(5)));
        let nested_add_op = Operand::Add(Box::new(Operand::Integer(3)), Box::new(add_op));
        assert_eq!(type_of(&nested_add_op, &schema), Some(SQLType::Integer));
    }

    #[test]
    fn gen_codes_for_the_simplest_select_statement() {
        let schema = get_schema();
        let sql = ParsedSQL::Select {
            table: None,
            operands: vec![Operand::Integer(42)],
        };
        let op_codes = gen_code(&sql, &schema);

        let expected = vec![OpCode::LoadInt(42), OpCode::StoreInt, OpCode::FlushRow];
        assert_eq!(op_codes, expected);
    }

    #[test]
    fn gen_codes_for_select_string_literal() {
        let schema = get_schema();
        let sql = ParsedSQL::Select {
            table: None,
            operands: vec![Operand::String("foo, bar".to_owned())],
        };
        let op_codes = gen_code(&sql, &schema);

        let expected = vec![
            OpCode::LoadStr("foo, bar".to_owned()),
            OpCode::StoreStr,
            OpCode::FlushRow,
        ];
        assert_eq!(op_codes, expected);
    }
}
