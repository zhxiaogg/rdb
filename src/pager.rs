use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Index, IndexMut, Range, RangeFrom};
use std::cell::{Ref, RefCell, RefMut};

use byteorder::{BigEndian, ByteOrder};
use btree::{BTree, BTreeInternalPage, BTreeLeafPage, BTreePage, CellIndex, PageType};

const PAGE_SIZE: usize = 4096;
const MAX_PAGE_PER_TABLE: usize = 100;
pub const ROW_SIZE: usize = 4 + 32 + 256;

const PAGE_TYPE_OFFSET: usize = 0;
const PAGE_TYPE_SIZE: usize = 1;
const IS_ROOT_OFFSET: usize = 1;
const IS_ROOT_SIZE: usize = 1;
const PARENT_POINTER_OFFSET: usize = 2;
const PARENT_POINTER_SIZE: usize = 4;
const COMMON_NODE_HEADER_SIZE: usize = PAGE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

const NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const NUM_CELLS_SIZE: usize = 4;

// for leaf page layout:
const NEXT_PAGE_OFFSET: usize = COMMON_NODE_HEADER_SIZE + NUM_CELLS_SIZE;
const NEXT_PAGE_SIZE: usize = 4;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + NUM_CELLS_SIZE + NEXT_PAGE_SIZE;

const CELL_OFFSET: usize = LEAF_NODE_HEADER_SIZE;
pub const KEY_SIZE: usize = 4;
const CELL_VALUE_SIZE: usize = ROW_SIZE;
pub const LEAF_NODE_CELL_SIZE: usize = KEY_SIZE + CELL_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// for internal page layout:
const RIGH_PAGE_INDEX_OFFSET: usize = NUM_CELLS_OFFSET + NUM_CELLS_SIZE;
const RIGHT_PAGE_INDEX_SIZE: usize = 4;

const INTERNAL_NODE_HEADER_SIZE: usize = RIGH_PAGE_INDEX_OFFSET + RIGHT_PAGE_INDEX_SIZE;
const KEY_INDEX_OFFSET: usize = INTERNAL_NODE_HEADER_SIZE;
const INDEX_SIZE: usize = 4;
const INTERNAL_NODE_CELL_SIZE: usize = INDEX_SIZE + KEY_SIZE;
const INTERNAL_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE;
const INTERNAL_NODE_MAX_CELLS: usize = INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE;

// constants for leaf page splitting:
const FIRST_HALF_NUM_CELLS: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
const SECOND_HALF_NUM_CELLS: usize = LEAF_NODE_MAX_CELLS - FIRST_HALF_NUM_CELLS;
const FIRST_HALF_PAGE_SIZE: usize = FIRST_HALF_NUM_CELLS * LEAF_NODE_CELL_SIZE;
const SECOND_HALF_CELLS_OFFSET: usize = CELL_OFFSET + FIRST_HALF_PAGE_SIZE;
const SECOND_HALF_PAGE_SIZE: usize = SECOND_HALF_NUM_CELLS * LEAF_NODE_CELL_SIZE;
// const CELL_END_OFFSET: usize = CELL_OFFSET + LEAF_NODE_MAX_CELLS * LEAF_NODE_CELL_SIZE;

// TODO: find a better place for this method
pub fn print_constants() {
    println!("Constants:");
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}

impl From<u8> for PageType {
    fn from(v: u8) -> PageType {
        if v == 0u8 {
            PageType::Internal
        } else if v == 1u8 {
            PageType::Leaf
        } else {
            panic!("wtf: invalid page type!")
        }
    }
}

pub type Page = Vec<u8>;
const RANGE_FOR_NUM_CELLS: RangeFrom<usize> = RangeFrom {
    start: NUM_CELLS_OFFSET,
};
const RANGE_FOR_PARENT_INDEX: RangeFrom<usize> = RangeFrom {
    start: PARENT_POINTER_OFFSET,
};
const RANGE_FOR_NEXT_PAGE: RangeFrom<usize> = RangeFrom {
    start: NEXT_PAGE_OFFSET,
};


fn range_for_internal_page_key(index: usize) -> RangeFrom<usize> {
    RangeFrom {
        start: KEY_INDEX_OFFSET + INDEX_SIZE + index * INTERNAL_NODE_CELL_SIZE,
    }
}

fn range_for_leaf_page_key(index: usize) -> RangeFrom<usize> {
    RangeFrom {
        start: CELL_OFFSET + index * LEAF_NODE_CELL_SIZE,
    }
}

fn range_for_internal_page_index(index: usize) -> RangeFrom<usize> {
    if index == INTERNAL_NODE_MAX_CELLS {
        RangeFrom {
            start: RIGH_PAGE_INDEX_OFFSET,
        }
    } else {
        RangeFrom {
            start: KEY_INDEX_OFFSET + index * INTERNAL_NODE_CELL_SIZE,
        }
    }
}

pub trait PageTrait {
    fn new_leaf_page(is_root: bool) -> Page;

    fn move_slice_internally(&mut self, from: usize, to: usize, len: usize);

    fn wrap_slice(&mut self, from: usize, buf: &Vec<u8>);
}

impl PageTrait for Page {
    fn new_leaf_page(is_root: bool) -> Page {
        let mut page = vec![0; PAGE_SIZE];
        page.set_page_type(PageType::Leaf);
        page.set_num_cells(0);
        page.set_is_root(is_root);
        page
    }

    fn move_slice_internally(&mut self, from: usize, to: usize, len: usize) {
        let mut vec = vec![0; len];
        {
            let slice = self.index(Range {
                start: from,
                end: from + len,
            }).clone();
            vec.clone_from_slice(slice);
        }
        let mut i = 0;
        for b in vec {
            self[to + i] = b;
            i += 1;
        }
    }

    fn wrap_slice(&mut self, from: usize, buf: &Vec<u8>) {
        let mut i = 0;
        for b in buf {
            self[from + i] = *b;
            i += 1;
        }
    }
}

impl BTreePage for Page {
    fn get_page_type(&self) -> PageType {
        let v = self[PAGE_TYPE_OFFSET];
        PageType::from(v)
    }

    fn set_page_type(&mut self, page_type: PageType) {
        self[PAGE_TYPE_OFFSET] = page_type as u8;
    }

    fn set_parent_page_index(&mut self, page_index: usize) {
        BigEndian::write_u32(self.index_mut(RANGE_FOR_PARENT_INDEX), page_index as u32);
    }

    fn get_parent_page_index(&self) -> usize {
        BigEndian::read_u32(self.index(RANGE_FOR_PARENT_INDEX)) as usize
    }

    fn is_root(&self) -> bool {
        self[IS_ROOT_OFFSET] == 1
    }

    fn set_is_root(&mut self, is_root: bool) {
        self[IS_ROOT_OFFSET] = if is_root { 1u8 } else { 0u8 };
    }

    fn get_num_cells(&self) -> u32 {
        BigEndian::read_u32(self.index(RANGE_FOR_NUM_CELLS))
    }

    fn set_num_cells(&mut self, num_cells: u32) {
        let max_num_cells = match self.get_page_type() {
            PageType::Leaf => LEAF_NODE_MAX_CELLS,
            PageType::Internal => INTERNAL_NODE_MAX_CELLS,
        };
        if (num_cells as usize) > max_num_cells {
            panic!("max number of cells exceeded!");
        }

        BigEndian::write_u32(self.index_mut(RANGE_FOR_NUM_CELLS), num_cells)
    }

    fn set_key_for_cell(&mut self, cell_index: usize, key: u32) {
        let range_from = match self.get_page_type() {
            PageType::Leaf => range_for_leaf_page_key(cell_index),
            PageType::Internal => range_for_internal_page_key(cell_index),
        };
        BigEndian::write_u32(self.index_mut(range_from), key)
    }

    fn get_key_for_cell(&self, cell_index: usize) -> u32 {
        let range_from = match self.get_page_type() {
            PageType::Leaf => range_for_leaf_page_key(cell_index),
            PageType::Internal => range_for_internal_page_key(cell_index),
        };
        BigEndian::read_u32(self.index(range_from))
    }

    fn find_cell_for_key(&self, key: u32) -> usize {
        let num_cells = self.get_num_cells();
        if num_cells == 0 {
            return 0;
        }

        // binary search
        let mut high = num_cells as usize;
        let mut index = 0usize;
        while index != high {
            let mid = (index + high) / 2;
            let curr_key = self.get_key_for_cell(mid);
            if curr_key == key {
                index = mid;
                break;
            } else if curr_key < key {
                index = mid + 1;
            } else {
                high = mid;
            }
        }
        return index;
    }
}

impl BTreeLeafPage for Page {
    fn pos_for_cell(cell_index: usize) -> usize {
        CELL_OFFSET + cell_index * LEAF_NODE_CELL_SIZE
    }

    fn get_next_page(&self) -> usize {
        BigEndian::read_u32(self.index(RANGE_FOR_NEXT_PAGE)) as usize
    }

    fn set_next_page(&mut self, next_page_index: usize) {
        BigEndian::write_u32(self.index_mut(RANGE_FOR_NEXT_PAGE), next_page_index as u32)
    }

    fn has_next_page(&self) -> bool {
        self.get_next_page() != 0
    }
}

impl BTreeInternalPage for Page {
    fn set_page_index(&mut self, index: usize, page_index: usize) {
        BigEndian::write_u32(
            self.index_mut(range_for_internal_page_index(index)),
            page_index as u32,
        )
    }

    fn get_page_index(&self, index: usize) -> usize {
        BigEndian::read_u32(self.index(range_for_internal_page_index(index))) as usize
    }

    fn find_page_for_key(&self, key: u32) -> usize {
        self.get_page_index(self.find_cell_for_key(key))
    }
}

pub struct Pager {
    file: RefCell<File>,
    pages: RefCell<Vec<Option<Page>>>,
    pub num_pages: usize,
    root_page_index: usize,
}

impl Pager {
    pub fn new(file: &str) -> Pager {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .unwrap();
        let file_size = file.metadata().unwrap().len();
        if file_size % (PAGE_SIZE as u64) != 0 {
            panic!("Db file is not a whole number of pages. Corrupt file.");
        }
        let num_pages = (file_size / (PAGE_SIZE as u64)) as usize;
        let pages = vec![None; MAX_PAGE_PER_TABLE];
        Pager {
            file: RefCell::new(file),
            pages: RefCell::new(pages),
            num_pages: num_pages,
            root_page_index: 0,
        }
    }

    pub fn flush(self: &mut Pager, page_index: usize) {
        let offset = page_index * PAGE_SIZE;
        if let Some(page) = self.pages.borrow()[page_index].as_ref() {
            let mut file = self.file.borrow_mut();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(page.as_ref()).unwrap();
        }
    }

    fn load(&self, page_index: usize) {
        let offset = page_index * PAGE_SIZE;
        let mut buf = vec![0; PAGE_SIZE];

        let mut file = self.file.borrow_mut();
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.read(buf.as_mut_slice()).unwrap();

        self.pages.borrow_mut()[page_index] = Some(buf);
    }

    pub fn page_for_read(self: &Pager, page_index: usize) -> Ref<Page> {
        if page_index >= self.num_pages {
            panic!("read EOF");
        } else if self.pages.borrow()[page_index].is_none() {
            self.load(page_index);
        }
        Ref::map(self.pages.borrow(), |pages| {
            pages[page_index].as_ref().unwrap()
        })
    }

    pub fn page_for_write(self: &mut Pager, page_index: usize) -> RefMut<Page> {
        if page_index > self.num_pages {
            panic!("skipped write to a page");
        } else if page_index == self.num_pages {
            // need a new page
            self.pages.borrow_mut()[page_index] = Some(Page::new_leaf_page(page_index == 0));
            self.num_pages += 1;
        } else if self.pages.borrow()[page_index].is_none() {
            // load page from file
            self.load(page_index);
        }
        RefMut::map(self.pages.borrow_mut(), |pages| {
            pages[page_index].as_mut().unwrap()
        })
    }

    // this method is designed for dev or test purpose only.
    pub fn debug_print(&self) {
        println!("Tree:");
        if self.num_pages > 0 {
            self.debug_print_for_page(0, "");
        }
    }

    fn debug_print_for_page(&self, page_index: usize, padding: &str) {
        let page = self.page_for_read(page_index);
        match page.get_page_type() {
            PageType::Leaf => {
                let num_cells = page.get_num_cells();
                println!("{}- leaf (size {})", padding, num_cells);
                for cell_index in 0..(num_cells as usize) {
                    println!("{}  - {}", padding, page.get_key_for_cell(cell_index));
                }
            }
            PageType::Internal => {
                let num_keys = page.get_num_cells() as usize;
                println!("{}- internal (size {})", padding, num_keys);
                for index in 0..num_keys {
                    let child_page_index = page.get_page_index(index);
                    let key = page.get_key_for_cell(index);
                    self.debug_print_for_page(child_page_index, &format!("{}  ", padding));
                    println!("{}- key {}", &format!("{}  ", padding), key);
                }
                let child_page_index = page.get_page_index(num_keys);
                self.debug_print_for_page(child_page_index, &format!("{}  ", padding));
            }
        }
    }

    fn search_key_in_page(&self, key: u32, page_index: usize) -> CellIndex {
        let page = self.page_for_read(page_index);
        match page.get_page_type() {
            PageType::Leaf => CellIndex::new(page_index, page.find_cell_for_key(key)),
            PageType::Internal => self.search_key_in_page(key, page.find_page_for_key(key)),
        }
    }

    fn insert_key_into_internal(
        &mut self,
        page_index: usize,
        key: u32,
        left_page_index: usize,
        right_page_index: usize,
    ) {
        let mut page = self.page_for_write(page_index);

        let num_cells = page.get_num_cells() as usize;
        let cell_index = page.find_cell_for_key(key);
        if num_cells >= INTERNAL_NODE_MAX_CELLS {
            //TODO: split internal node

        } else if cell_index < num_cells {
            // the right most page index should be moved first.
            let last_page_index = page.get_page_index(num_cells);
            page.set_page_index(num_cells + 1, last_page_index);

            // and then move cells for insertion space
            for index in (cell_index..num_cells).rev() {
                let from = index * INTERNAL_NODE_CELL_SIZE + KEY_INDEX_OFFSET;
                let to = from + INTERNAL_NODE_CELL_SIZE;
                page.move_slice_internally(from, to, INTERNAL_NODE_CELL_SIZE);
            }
        }

        page.set_num_cells((num_cells as u32) + 1);
        page.set_key_for_cell(cell_index, key);
        page.set_page_index(cell_index, left_page_index);
        page.set_page_index(cell_index + 1, right_page_index);
    }

    /**
     * Split a leaf page identified by given page_index.
     * If the given page is root page, then two new page will be created, and the
     * original page will be emptied and relocated. Otherwise only one new page will
     * be created.
     * This method returns a optinal relocated page_index (if original page is root page) and
     * the newly created page index.
     * TODO: bytes move not efficient!
     * TODO: consider not return the newly created page index, and let the caller search again?
     **/
    fn split_leaf_page(&mut self, page_index: usize) {
        let mut second_half_buf = vec![0u8; SECOND_HALF_PAGE_SIZE];
        let mut first_half_buf: Option<Vec<u8>> = None;
        let new_key;
        // copy bytes into vectors, which is inefficient
        //TODO: inefficient copy of bytes

        {
            let mut original_page = self.page_for_write(page_index);
            new_key = original_page.get_key_for_cell(FIRST_HALF_NUM_CELLS - 1);
            second_half_buf.clone_from_slice(original_page.index(Range {
                start: SECOND_HALF_CELLS_OFFSET,
                end: SECOND_HALF_CELLS_OFFSET + SECOND_HALF_PAGE_SIZE,
            }));
            if original_page.is_root() {
                let mut buf = vec![0u8; FIRST_HALF_PAGE_SIZE];
                buf.clone_from_slice(original_page.index(Range {
                    start: CELL_OFFSET,
                    end: CELL_OFFSET + FIRST_HALF_PAGE_SIZE,
                }));
                first_half_buf = Some(buf);

                // reset original root page
                original_page.set_page_type(PageType::Internal);
                original_page.set_num_cells(0);
            } else {
                original_page.set_num_cells(FIRST_HALF_NUM_CELLS as u32);
            }
        }

        // create a new leaf page if the original page is root
        let (parent_page_index, left_page_index) = match first_half_buf {
            None => (
                self.page_for_read(page_index).get_parent_page_index(),
                page_index,
            ),
            Some(buf) => {
                let left_page_index = self.num_pages;
                let mut left_page = self.page_for_write(left_page_index);
                left_page.wrap_slice(CELL_OFFSET, &buf);
                left_page.set_num_cells(FIRST_HALF_NUM_CELLS as u32);
                left_page.set_next_page(left_page_index + 1);
                left_page.set_parent_page_index(page_index);
                (page_index, left_page_index)
            }
        };

        // create a splitted page, and copy second half of page data into it
        let right_page_index = self.num_pages;
        {
            let mut right_page = self.page_for_write(right_page_index);
            right_page.wrap_slice(CELL_OFFSET, &second_half_buf);
            right_page.set_num_cells(SECOND_HALF_NUM_CELLS as u32);
            right_page.set_next_page(0);
            right_page.set_parent_page_index(parent_page_index);
        }

        // update parent node
        self.insert_key_into_internal(
            parent_page_index,
            new_key,
            left_page_index,
            right_page_index,
        );
    }

    fn write_key(&mut self, key: u32, page_index: usize, cell_index: usize) {
        let mut page = self.page_for_write(page_index);
        page.set_key_for_cell(cell_index, key);
        let num_cells = page.get_num_cells();
        page.set_num_cells((num_cells + 1) as u32);
    }
}

impl BTree for Pager {
    fn search_key(&self, key: u32) -> CellIndex {
        if self.num_pages == 0 {
            CellIndex::new(0, 0)
        } else {
            self.search_key_in_page(key, self.root_page_index)
        }
    }

    fn insert_key(&mut self, key: u32) -> Result<CellIndex, String> {
        let CellIndex {
            page_index,
            cell_index,
        } = self.search_key(key);
        let num_cells = match self.num_pages {
            0 => 0,
            _ => self.page_for_read(page_index).get_num_cells() as usize,
        };

        if num_cells >= LEAF_NODE_MAX_CELLS {
            // split page
            self.split_leaf_page(page_index);
            return self.insert_key(key);
        } else if cell_index < num_cells {
            let mut page = self.page_for_write(page_index);
            if page.get_key_for_cell(cell_index) == key {
                return Result::Err("Error: Duplicate key.".to_owned());
            }
            // need move existed cells
            for cell_index in (cell_index..num_cells).rev() {
                let cell_pos = Page::pos_for_cell(cell_index);
                let new_cell_pos = cell_pos + LEAF_NODE_CELL_SIZE;
                page.move_slice_internally(cell_pos, new_cell_pos, LEAF_NODE_CELL_SIZE);
            }
        }

        self.write_key(key, page_index, cell_index);
        Result::Ok(CellIndex::new(page_index, cell_index))
    }
}
