use sql::SQLType;
use std::collections::HashMap;

pub struct Schema {
    columns: HashMap<String, (SQLType, usize)>,
    indexed_columns: Vec<String>,
}

impl Schema {
    pub fn new() -> Schema {
        let mut schema = Schema {
            columns: HashMap::new(),
            indexed_columns: Vec::new(),
        };
        schema.add_column("id", SQLType::Integer);
        schema.add_column("name", SQLType::String);
        schema.add_column("email", SQLType::String);
        schema
    }

    pub fn add_column(&mut self, name: &str, column_type: SQLType) {
        let index = self.indexed_columns.len();
        self.columns.insert(name.to_owned(), (column_type, index));
        self.indexed_columns.push(name.to_owned());
    }

    pub fn index_of(&self, index: usize) -> Option<String> {
        if index < self.indexed_columns.len() {
            Some(self.indexed_columns[index].to_owned())
        } else {
            None
        }
    }

    pub fn get_column_type(&self, column: &str) -> Option<SQLType> {
        self.columns.get(column).map(|c| c.0.clone())
    }

    pub fn get_index_of(&self, column: &str) -> Option<usize> {
        self.columns.get(column).map(|c| c.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn get_schema() -> Schema {
        Schema::new()
    }

    #[test]
    fn get_column_type() {
        let schema = get_schema();
        assert_eq!(schema.get_column_type("id"), Some(SQLType::Integer));
        assert_eq!(schema.get_column_type("email"), Some(SQLType::String));
    }

    #[test]
    fn get_index_of() {
        let schema = get_schema();
        assert_eq!(schema.get_index_of("id"), Some(0));
        assert_eq!(schema.get_index_of("email"), Some(2));
    }

    #[test]
    fn index_of() {
        let schema = get_schema();
        assert_eq!(schema.index_of(0), Some("id".to_owned()));
        assert_eq!(schema.index_of(1), Some("name".to_owned()));
    }
}
