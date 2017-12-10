extern crate byteorder;
#[macro_use]
extern crate log;
#[macro_use]
extern crate nom;

use std::io;
use std::process;
use std::io::Write;
use std::env;

mod table;
mod pager;
mod btree;
mod vm;
mod sql;
mod codegen;

use table::Table;
use pager::{DbOption, Pager};
use btree::BTree;
use vm::{Statement, VM};

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
                Result::Ok(_) => {}
                Result::Err(msg) => println!("{}", &msg),
            }
            continue;
        }

        match Statement::prepare(&input_buffer.trim()) {
            Result::Ok(mut statement) => match statement.execute(&mut table) {
                Result::Ok(_) => println!("Executed."),
                Result::Err(msg) => println!("{}", &msg),
            },
            Result::Err(msg) => println!("{}", &msg),
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

fn do_meta_command(input_buffer: &str, table: &mut Table) -> Result<(), String> {
    if input_buffer.eq(".exit") {
        table.close();
        process::exit(0)
    } else if input_buffer.eq(".constants") {
        table.tree.config.print_constants();
        Result::Ok(())
    } else if input_buffer.eq(".btree_internal") {
        table.debug_print(true);
        Result::Ok(())
    } else if input_buffer.eq(".btree") {
        table.debug_print(false);
        Result::Ok(())
    } else {
        Result::Err(format!("Unrecognized command: {}", input_buffer))
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
