use sql::SQLType;
use std::collections::HashMap;

pub struct Schema {
    columns: HashMap<String, SQLType>,
}

impl Schema {
    pub fn new() -> Schema {
        let mut map = HashMap::new();
        map.insert("id".to_owned(), SQLType::Integer);
        map.insert("name".to_owned(), SQLType::String);
        map.insert("email".to_owned(), SQLType::String);
        Schema { columns: map }
    }

    pub fn get_column_type(&self, column: &String) -> Option<SQLType> {
        self.columns.get(column).map(|t| t.clone())
    }
}
