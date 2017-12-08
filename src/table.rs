use std::ops::{Index, IndexMut, Range, RangeFrom};
use byteorder::{BigEndian, ByteOrder};
use std::cell::RefCell;
use std::rc::Rc;

use pager::Page;
use btree::{BTree, BTreeLeafPage, BTreePage, BTreeTrait, CellIndex, KEY_SIZE, ROW_SIZE};

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
    pub tree: BTree,
}

impl Table {
    /**
     * ideally there is one and only one b+tree for a table, and
     * will be zero or more b-tree for table indices.
     **/
    pub fn new(tree: BTree) -> Table {
        return Table { tree: tree };
    }

    pub fn close(self: &mut Table) {
        for page_index in 0..self.tree.pager.num_pages {
            self.tree.pager.flush(page_index);
        }
    }

    pub fn select_cursor(&self) -> SelectCursor {
        let CellIndex {
            page_index,
            cell_index,
        } = self.tree.search_key(0);
        SelectCursor::new(&self.tree, page_index, cell_index)
    }

    pub fn insert_cursor(&mut self, key: u32) -> UpdateCursor {
        UpdateCursor::new(&mut self.tree, key)
    }

    // TODO: remove this method
    pub fn debug_print(&self) {
        self.tree.debug_print();
    }
}

pub struct SelectCursor<'a> {
    tree: &'a BTree,
    page_index: usize,
    cell_index: usize,
}

impl<'a> SelectCursor<'a> {
    fn new(tree: &'a BTree, page_index: usize, cell_index: usize) -> SelectCursor<'a> {
        SelectCursor {
            tree: tree,
            page_index: page_index,
            cell_index: cell_index,
        }
    }

    fn get_page(&self) -> Rc<RefCell<Page>> {
        self.tree.pager.page_for_read(self.page_index)
    }

    pub fn end_of_table(&self) -> bool {
        self.tree.pager.num_pages == 0 || self.is_last_page()
    }

    fn is_last_page(&self) -> bool {
        let rc_page = self.get_page();
        let page = &rc_page.borrow();
        (self.cell_index >= (page.get_num_cells() as usize) && !page.has_next_page())
    }

    pub fn advance(&mut self) {
        let rc_page = self.get_page();
        let page = &rc_page.borrow();
        let num_cells = page.get_num_cells() as usize;
        self.cell_index += 1;
        if self.cell_index >= num_cells && page.has_next_page() {
            let next_page_index = page.get_next_page();
            self.page_index = next_page_index;
            self.cell_index = 0;
        }
    }

    pub fn get(&self) -> Row {
        let cell_pos = Page::pos_for_cell(self.cell_index);
        let rc_page = self.tree.pager.page_for_read(self.page_index);
        let page = &rc_page.borrow();
        Row::deserialize(page, cell_pos + KEY_SIZE)
    }
}

pub struct UpdateCursor<'a> {
    tree: &'a mut BTree,
    key: u32,
}

impl<'a> UpdateCursor<'a> {
    fn new(tree: &'a mut BTree, key: u32) -> UpdateCursor<'a> {
        UpdateCursor {
            tree: tree,
            key: key,
        }
    }

    pub fn save(&mut self, row: &Row) -> Result<(), String> {
        self.tree.insert_key(self.key).map(|cell_index| {
            let cell_pos = Page::pos_for_cell(cell_index.cell_index);
            let rc_page = self.tree.pager.page_for_write(cell_index.page_index);
            let page = &mut rc_page.borrow_mut();
            Row::serialize(row, page, cell_pos + KEY_SIZE);
        })
    }
}
