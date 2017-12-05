use std::ops::{Index, IndexMut, Range, RangeFrom};
use byteorder::{BigEndian, ByteOrder};
use std::cell::Ref;

use pager::{LeafPage, Page, PageTrait, Pager, KEY_SIZE, ROW_SIZE};

pub struct Row {
    pub id: u32,
    pub username: String,
    pub email: String,
}

impl Row {
    fn serialize(row: &Row, page: &mut Page, pos: usize) {
        BigEndian::write_u32(page.index_mut(RangeFrom { start: pos }), row.id);
        Row::write_string(page, pos + 4, &row.username, 32);
        Row::write_string(page, pos + 36, &row.email, 256);
    }

    fn deserialize(buf: &Vec<u8>, pos: usize) -> Row {
        let mut bytes = vec![0; ROW_SIZE];
        bytes.clone_from_slice(buf.index(Range {
            start: pos,
            end: pos + ROW_SIZE,
        }));

        let mut position = 0;
        let id = BigEndian::read_u32(bytes.as_slice());
        position += 4;
        let username = Row::read_string(&bytes, position, 32);
        position += 32;
        let email = Row::read_string(&bytes, position, 256);
        Row {
            id: id,
            username: username,
            email: email,
        }
    }

    fn write_string(buf: &mut Vec<u8>, pos: usize, s: &str, length: usize) {
        let bytes = s.as_bytes();

        let mut i = 0;
        for b in bytes {
            buf[pos + i] = *b;
            i += 1;
        }
        while i < length {
            buf[pos + i] = 0;
            i += 1;
        }
    }

    fn read_string(buf: &Vec<u8>, pos: usize, length: usize) -> String {
        let mut end = pos;
        while ((end - pos) < length) && (buf[end] != 0) {
            end += 1;
        }
        let mut bytes = vec![0; end - pos];
        bytes.clone_from_slice(buf.index(Range {
            start: pos,
            end: end,
        }));
        return String::from_utf8(bytes).unwrap();
    }
}


pub struct Table {
    pager: Pager,
}

impl Table {
    pub fn new(file: &str) -> Table {
        let pager = Pager::new(file);
        return Table { pager: pager };
    }

    pub fn close(self: &mut Table) {
        for page_index in 0..self.pager.num_pages {
            self.pager.flush(page_index);
        }
    }

    // TODO: how to determin whether a table is full or not?
    pub fn is_full(self: &Table) -> bool {
        return false;
    }

    //TODO: select cursor should not pass a mutable table
    pub fn select_cursor(self: &mut Table) -> Cursor {
        let (page_index, cell_index) = self.pager.find_cell(0);
        Cursor::new(self, page_index, cell_index)
    }

    pub fn insert_cursor(self: &mut Table, key: u32) -> Cursor {
        let (page_index, cell_index) = self.pager.find_cell(key);
        Cursor::new(self, page_index, cell_index)
    }

    pub fn debug_print(&mut self) {
        self.pager.debug_print();
    }
}

pub struct Cursor<'a> {
    table: &'a mut Table,
    page_index: usize,
    cell_index: usize,
}

impl<'a> Cursor<'a> {
    fn new(table: &'a mut Table, page_index: usize, cell_index: usize) -> Cursor<'a> {
        Cursor {
            table: table,
            page_index: page_index,
            cell_index: cell_index,
        }
    }

    /**
     * short hand for get current page
     **/
    fn page_for_read(&self) -> Ref<Page> {
        self.table.pager.page_for_read(self.page_index)
    }

    pub fn end_of_table(self: &mut Cursor<'a>) -> bool {
        self.table.pager.num_pages == 0
            || (self.cell_index >= (self.page_for_read().get_num_cells() as usize)
                && self.page_for_read().get_next_page() == 0)
    }

    pub fn advance(self: &mut Cursor<'a>) {
        let num_cells = self.page_for_read().get_num_cells() as usize;
        self.cell_index += 1;
        if self.cell_index >= num_cells && self.page_for_read().has_next_page() {
            let next_page_index = self.page_for_read().get_next_page();
            self.page_index = next_page_index;
            self.cell_index = 0;
        }
    }

    pub fn get(self: &mut Cursor<'a>) -> Row {
        let cell_pos = Page::pos_for_cell(self.cell_index);
        Row::deserialize(&self.page_for_read(), cell_pos + KEY_SIZE)
    }

    pub fn save(self: &mut Cursor<'a>, key: u32, row: &Row) -> Result<(), String> {
        self.table
            .pager
            .insert_key(key, self.page_index, self.cell_index)
            .map(|(page_index, cell_index)| {
                let cell_pos = Page::pos_for_cell(cell_index);
                let page = &mut self.table.pager.page_for_write(page_index);
                Row::serialize(row, page, cell_pos + KEY_SIZE);
            })
    }
}
