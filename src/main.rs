extern crate byteorder;

use std::io;
use std::process;
use std::io::Write;
use std::env;

mod table;
mod pager;
mod btree;

use table::{Row, Table};
use pager::{DbOption, Pager};
use btree::BTree;

const DEFAULT_PAGE_SIZE: usize = 4096;
const DEFAULT_DB_FILE: &str = "default.rdb";
const ENV_PAGE_SIZE: &str = "RDB_PAGE_SIZE";

fn main() {
    let pager = create_pager();
    let tree = BTree::new(pager);

    //TODO: print rdb info
    let mut table = Table::new(tree);

    let mut input_buffer = String::new();
    loop {
        print_prompt();

        read_input(&mut input_buffer);

        if input_buffer.starts_with(".") {
            match do_meta_command(&input_buffer.trim(), &mut table) {
                ExecuteResult::Ok => {}
                ExecuteResult::Err(msg) => println!("{}", &msg),
            }
            continue;
        }

        match prepare_statement(&input_buffer.trim()) {
            PrepareResult::Ok(statement) => match execute_statement(&statement, &mut table) {
                ExecuteResult::Ok => println!("Executed."),
                ExecuteResult::Err(msg) => println!("{}", &msg),
            },
            PrepareResult::Err(msg) => println!("{}", &msg),
        }
    }
}

fn create_pager() -> Pager {
    let db = match env::args().nth(1) {
        Some(file) => file,
        None => String::from(DEFAULT_DB_FILE),
    };
    let mut page_size = DEFAULT_PAGE_SIZE;
    if let Some((_, v)) = env::vars().find(|r| r.0.eq(ENV_PAGE_SIZE)) {
        page_size = u32::from_str_radix(&v, 10)
            .expect(&format!("invalid value for {}", ENV_PAGE_SIZE)) as usize;
    };

    let db_option = DbOption {
        page_size: page_size,
    };
    Pager::new(db.as_str(), db_option)
}

fn do_meta_command(input_buffer: &str, table: &mut Table) -> ExecuteResult {
    if input_buffer.eq(".exit") {
        table.close();
        process::exit(0)
    } else if input_buffer.eq(".constants") {
        table.tree.config.print_constants();
        ExecuteResult::Ok
    } else if input_buffer.eq(".btree_internal") {
        table.debug_print(true);
        ExecuteResult::Ok
    } else if input_buffer.eq(".btree") {
        table.debug_print(false);
        ExecuteResult::Ok
    } else {
        ExecuteResult::Err(format!("Unrecognized command: {}", input_buffer))
    }
}

enum StatementType {
    SELECT,
    INSERT,
}

struct Statement {
    kind: StatementType,
    row_to_insert: Option<Row>,
}

enum PrepareResult {
    Ok(Statement),
    Err(String),
}

enum ExecuteResult {
    Ok,
    Err(String),
}


fn prepare_statement(input_buffer: &str) -> PrepareResult {
    if input_buffer.starts_with("select") {
        PrepareResult::Ok(Statement {
            kind: StatementType::SELECT,
            row_to_insert: None,
        })
    } else if input_buffer.starts_with("insert") {
        let parts: Vec<&str> = input_buffer.trim().splitn(4, ' ').collect();
        if parts.len() != 4 {
            PrepareResult::Err(input_buffer.to_owned())
        } else {
            let id = i32::from_str_radix(parts[1], 10).unwrap();
            if id < 0 {
                return PrepareResult::Err("ID must be positive.".to_owned());
            }
            let username = String::from(parts[2]);
            let email = String::from(parts[3]);
            if username.len() > 32 || email.len() > 256 {
                return PrepareResult::Err("String is too long.".to_owned());
            }
            let statement = Statement {
                kind: StatementType::INSERT,
                row_to_insert: Some(Row {
                    id: id as u32,
                    username: username,
                    email: email,
                }),
            };
            PrepareResult::Ok(statement)
        }
    } else {
        PrepareResult::Err(format!("Unrecognized command: {}", input_buffer).to_owned())
    }
}

fn execute_statement(statement: &Statement, table: &mut Table) -> ExecuteResult {
    match statement.kind {
        StatementType::SELECT => {
            let mut cursor = table.select_cursor();
            while !cursor.end_of_table() {
                let row = cursor.get();
                println!("({}, {}, {})", row.id, &row.username, &row.email);
                cursor.advance();
            }
            ExecuteResult::Ok
        }
        StatementType::INSERT => {
            if let Some(r) = statement.row_to_insert.as_ref() {
                match table.insert_cursor(r.id).save(r) {
                    Result::Ok(()) => ExecuteResult::Ok,
                    Result::Err(msg) => ExecuteResult::Err(msg),
                }
            } else {
                ExecuteResult::Ok
            }
        }
    }
}

fn print_prompt() {
    print!("rdb > ");
    io::stdout().flush().unwrap();
}

fn read_input(input_buffer: &mut String) {
    input_buffer.clear();
    io::stdin()
        .read_line(input_buffer)
        .expect("read input error.");
}
