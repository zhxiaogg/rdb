use std::ops::{Range, Index, IndexMut};

use byteorder::{BigEndian, ByteOrder};

pub struct Row {
    pub id: i32,
    pub username: String,
    pub email: String
}

const PAGE_SIZE:usize = 4096;
const MAX_PAGE_PER_TABLE:usize = 100;
const ROW_SIZE:usize = 4 + 32 + 256;

type Page = Vec<u8>;
pub struct Table {
    pub pages:Vec<Page>,
    pub num_rows: usize
}

impl Table {
    pub fn new() -> Table {
        return Table {pages: Vec::with_capacity(MAX_PAGE_PER_TABLE), num_rows: 0};
    }

    pub fn max_rows(self:&Table) -> usize {
        return PAGE_SIZE / ROW_SIZE * MAX_PAGE_PER_TABLE;
    }

    pub fn insert(self:&mut Table, row:&Row) {
        let bytes = Table::serialize(row);
        {
            let row_index = self.num_rows;
            let (page, mut pos) = self.row_slot_for_write(row_index);
            for b in bytes {
                page[pos] = b;
                pos+=1;
            }
        }
        self.num_rows += 1;
    }

    pub fn get_row(self: &Table, row_index:usize) -> Row {
        let (page, pos) = self.row_slot_for_read(row_index);
        let mut position = pos;
        let id = BigEndian::read_i32(page.as_slice());
        position += 4;
        let username = Table::read_string(&page, position, 32);
        position += 32;
        let email = Table::read_string(&page, position, 256);

        return Row {id: id, username: username, email: email};
    }

    fn row_slot_for_write(self:&mut Table, row_index:usize) -> (&mut Page, usize) {
        let rows_per_page = PAGE_SIZE / ROW_SIZE;
        let page_num = row_index / rows_per_page;
        while self.pages.len() < page_num + 1 {
            self.pages.push(vec![0; PAGE_SIZE]);
        }
        return (&mut self.pages[page_num], ROW_SIZE * (row_index % rows_per_page));
    }

    fn row_slot_for_read(self: &Table, row_index:usize) -> (&Page, usize) {
        let rows_per_page = PAGE_SIZE / ROW_SIZE;
        let page_num = row_index / rows_per_page;
        return (&self.pages[page_num], ROW_SIZE * (row_index % rows_per_page));
    }

    fn serialize(row:&Row) -> Vec<u8> {
        let mut buf:Vec<u8> = vec![0;ROW_SIZE];
        BigEndian::write_i32(&mut buf.index_mut(Range{start: 0, end: 4}), row.id);
        Table::write_string(&mut buf, 4, &row.username, 32);
        Table::write_string(&mut buf, 36, &row.email, 256);
        return buf;
    }

    fn write_string(buf:&mut Vec<u8>, pos:usize, s:&str, length:usize) {
        let bytes = s.as_bytes();
        let mut vec = vec![0;bytes.len()];
        vec.copy_from_slice(bytes);

        let mut i = 0;
        for b in vec {
            buf[pos+i] = b;
            i+=1;
        }
        while i < length {
            buf[pos+i] = 0;
            i+=1;
        }
    }

    fn read_string(buf:&Vec<u8>, pos:usize, length:usize) -> String {
        let mut end = pos;
        while ((end - pos) < length) && (buf[end] != 0)  {
            end+=1;
        }
        let mut bytes = vec![0;end - pos];
        bytes.clone_from_slice(buf.index(Range{start: pos, end: end}));
        return String::from_utf8(bytes).unwrap();
    }
}
