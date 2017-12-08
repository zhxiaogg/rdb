use pager::{Page, PageTrait, Pager, PAGE_SIZE};
use std::ops::{Index, IndexMut, Range, RangeFrom};

use byteorder::{BigEndian, ByteOrder};

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

// constants for leaf page split:
const FIRST_HALF_NUM_CELLS: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
const SECOND_HALF_NUM_CELLS: usize = LEAF_NODE_MAX_CELLS - FIRST_HALF_NUM_CELLS;
const FIRST_HALF_PAGE_SIZE: usize = FIRST_HALF_NUM_CELLS * LEAF_NODE_CELL_SIZE;
const SECOND_HALF_CELLS_OFFSET: usize = CELL_OFFSET + FIRST_HALF_PAGE_SIZE;
const SECOND_HALF_PAGE_SIZE: usize = SECOND_HALF_NUM_CELLS * LEAF_NODE_CELL_SIZE;
// const CELL_END_OFFSET: usize = CELL_OFFSET + LEAF_NODE_MAX_CELLS * LEAF_NODE_CELL_SIZE;

// constants for internal page split:
const INTERNAL_FIRST_HALF_NUM_CELLS: usize = (INTERNAL_NODE_MAX_CELLS + 1) / 2;
const INTERNAL_SECOND_HALF_NUM_CELLS: usize =
    INTERNAL_NODE_MAX_CELLS - INTERNAL_FIRST_HALF_NUM_CELLS;

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

pub trait BTreeTrait {
    fn search_key(&self, key: u32) -> CellIndex;

    /**
     * this method will insert key and return the inserted cell index.
     **/
    fn insert_key(&mut self, key: u32) -> Result<CellIndex, String>;
}

pub trait BTreePage {
    fn init_as_leaf_page(&mut self, is_root: bool);

    fn init_as_internal_page(&mut self, is_root: bool);

    fn get_page_type(&self) -> PageType;

    fn set_page_type(&mut self, page_type: PageType);

    fn set_parent_page_index(&mut self, page_index: usize);

    fn get_parent_page_index(&self) -> usize;

    fn is_root(&self) -> bool;

    fn set_is_root(&mut self, is_root: bool);

    fn get_num_cells(&self) -> u32;

    fn set_num_cells(&mut self, num_cells: u32);

    fn set_key_for_cell(&mut self, cell_index: usize, key: u32);

    fn get_key_for_cell(&self, cell_index: usize) -> u32;

    /**
     * returns cell index
     **/
    fn find_cell_for_key(&self, key: u32) -> usize;
}

//TODO: update num cells using `usize`
pub trait BTreeLeafPage {
    fn pos_for_cell(cell_index: usize) -> usize;

    fn get_next_page(&self) -> usize;

    fn set_next_page(&mut self, next_page_index: usize);

    fn has_next_page(&self) -> bool;
}

pub trait BTreeInternalPage {
    fn set_page_index(&mut self, index: usize, page_index: usize);

    fn get_page_index(&self, index: usize) -> usize;

    fn find_page_for_key(&self, key: u32) -> usize;
}

pub enum PageType {
    Internal = 0,
    Leaf = 1,
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

pub struct CellIndex {
    pub page_index: usize,
    pub cell_index: usize,
}

impl CellIndex {
    pub fn new(page_index: usize, cell_index: usize) -> CellIndex {
        CellIndex {
            page_index: page_index,
            cell_index: cell_index,
        }
    }
}

pub struct BTree {
    pub pager: Pager,
    root_page_index: usize,
}

impl BTree {
    pub fn new(pager: Pager) -> BTree {
        BTree {
            pager: pager,
            root_page_index: 0,
        }
    }

    fn search_key_in_page(&self, key: u32, page_index: usize) -> CellIndex {
        let rc_page = self.pager.page_for_read(page_index);
        let page = rc_page.borrow();
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
        let rc_page = self.pager.page_for_write(page_index);
        let mut page = rc_page.borrow_mut();

        let num_cells = page.get_num_cells() as usize;
        let cell_index = page.find_cell_for_key(key);
        if num_cells >= INTERNAL_NODE_MAX_CELLS {
            if page.is_root() {
                self.split_root_internal_page_and_insert_key(
                    page_index,
                    key,
                    left_page_index,
                    right_page_index,
                );
            } else {
                self.split_internal_page_and_insert_key(
                    page_index,
                    key,
                    left_page_index,
                    right_page_index,
                );
            }
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

    fn split_root_internal_page_and_insert_key(
        &mut self,
        page_index: usize,
        key: u32,
        left_page_index: usize,
        right_page_index: usize,
    ) {
        let rc_page = self.pager.page_for_write(page_index);
        let mut page = rc_page.borrow_mut();

        let new_right_page_index = self.pager.next_page_index();
        let rc_new_page = self.pager.page_for_write(new_right_page_index);
        let mut new_page = rc_new_page.borrow_mut();
        new_page.init_as_internal_page(false);
        new_page.set_parent_page_index(page_index);

        let new_left_page_index = self.pager.next_page_index();
        let rc_new_left_page = self.pager.page_for_write(new_left_page_index);
        let mut new_left_page = rc_new_left_page.borrow_mut();
        new_left_page.init_as_internal_page(false);
        new_left_page.set_parent_page_index(page_index);

        // move cells to new pages
        let mut inserted = false;
        let num_cells = page.get_num_cells() as usize;
        let right_most_page = page.get_page_index(num_cells);
        new_page.set_page_index(INTERNAL_SECOND_HALF_NUM_CELLS, right_most_page);
        for cell_index in (0..num_cells).rev() {
            let cell_key = page.get_key_for_cell(cell_index);

            if cell_index >= FIRST_HALF_NUM_CELLS {
                // move to new page
                let new_cell_index = cell_index - FIRST_HALF_NUM_CELLS;
                if key <= cell_key && !inserted {
                    new_page.set_key_for_cell(new_cell_index, key);
                    new_page.set_page_index(new_cell_index, left_page_index);
                    new_page.set_page_index(new_cell_index + 1, right_page_index);
                    inserted = true;
                } else {
                    let cell_page_index = page.get_page_index(cell_index);
                    new_page.set_key_for_cell(new_cell_index, cell_key);
                    new_page.set_page_index(new_cell_index + 1, cell_page_index);
                }
            } else {
                // move cells in first page
                if key <= cell_key && !inserted {
                    new_left_page.set_key_for_cell(cell_index, key);
                    new_left_page.set_page_index(cell_index, left_page_index);
                    new_left_page.set_page_index(cell_index + 1, right_page_index);
                    inserted = true;
                } else {
                    let new_cell_index = cell_index + 1;
                    let cell_page_index = page.get_page_index(cell_index);
                    new_left_page.set_key_for_cell(new_cell_index, cell_key);
                    new_left_page.set_page_index(new_cell_index + 1, cell_page_index);
                }
            }
        }

        let max_key = new_left_page.get_key_for_cell(new_left_page.get_num_cells() as usize - 1);
        page.init_as_internal_page(true);
        page.set_page_index(0, new_left_page_index);
        page.set_key_for_cell(0, max_key);
        page.set_page_index(1, new_right_page_index);
    }

    /**
     * see also #split_root_internal_page_and_insert_key
     **/
    fn split_internal_page_and_insert_key(
        &mut self,
        page_index: usize,
        key: u32,
        left_page_index: usize,
        right_page_index: usize,
    ) {
        let rc_page = self.pager.page_for_write(page_index);
        let mut page = rc_page.borrow_mut();
        let parent_page_index = page.get_parent_page_index();

        let new_page_index = self.pager.next_page_index();
        let rc_new_page = self.pager.page_for_write(new_page_index);
        let mut new_page = rc_new_page.borrow_mut();
        new_page.init_as_internal_page(false);
        new_page.set_parent_page_index(parent_page_index);

        // move half cells to new page, stop once the key get inserted
        // here we choose moving by deserialize & serialize
        // however we could also choose moving bytes directly.
        let mut inserted = false;
        let num_cells = page.get_num_cells() as usize;
        let right_most_page = page.get_page_index(num_cells);
        new_page.set_page_index(INTERNAL_SECOND_HALF_NUM_CELLS, right_most_page);
        for cell_index in (0..num_cells).rev() {
            let cell_key = page.get_key_for_cell(cell_index);

            if cell_index >= FIRST_HALF_NUM_CELLS {
                // move to new page
                let new_cell_index = cell_index - FIRST_HALF_NUM_CELLS;
                if key <= cell_key && !inserted {
                    new_page.set_key_for_cell(new_cell_index, key);
                    new_page.set_page_index(new_cell_index, left_page_index);
                    new_page.set_page_index(new_cell_index + 1, right_page_index);
                    inserted = true;
                } else {
                    let cell_page_index = page.get_page_index(cell_index);
                    new_page.set_key_for_cell(new_cell_index, cell_key);
                    new_page.set_page_index(new_cell_index + 1, cell_page_index);
                }
            } else if inserted {
                break;
            } else {
                // move cells in first page
                if key <= cell_key {
                    page.set_key_for_cell(cell_index, key);
                    page.set_page_index(cell_index, left_page_index);
                    page.set_page_index(cell_index + 1, right_page_index);
                    break;
                } else {
                    let new_cell_index = cell_index + 1;
                    let cell_page_index = page.get_page_index(cell_index);
                    page.set_key_for_cell(new_cell_index, cell_key);
                    page.set_page_index(new_cell_index + 1, cell_page_index);
                }
            }
        }

        // update parent
        let max_left_key = page.get_key_for_cell(page.get_num_cells() as usize - 1);
        self.insert_key_into_internal(parent_page_index, max_left_key, page_index, new_page_index);
    }

    /**
     * Split a leaf page identified by given page_index.
     * If the given page is root page, then two new page will be created, and the
     * original page will be emptied and relocated. Otherwise only one new page will
     * be created.
     * This method returns a optinal relocated page_index (if original page is root page) and
     * the newly created page index.
     * TODO: bytes move not efficient!
     **/
    fn split_leaf_page(&mut self, page_index: usize) {
        let mut second_half_buf = vec![0u8; SECOND_HALF_PAGE_SIZE];
        let mut first_half_buf: Option<Vec<u8>> = None;
        let new_key;
        // copy bytes into vectors, which is inefficient
        //TODO: inefficient copy of bytes
        {
            let rc_page = self.pager.page_for_write(page_index);
            let mut original_page = rc_page.borrow_mut();
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
                original_page.init_as_internal_page(true);
            } else {
                original_page.set_num_cells(FIRST_HALF_NUM_CELLS as u32);
            }
        }

        // create a new leaf page if the original page is root
        let (parent_page_index, left_page_index) = match first_half_buf {
            None => {
                let rc_page = self.pager.page_for_read(page_index);
                let page = rc_page.borrow();
                (page.get_parent_page_index(), page_index)
            }
            Some(buf) => {
                let left_page_index = self.pager.num_pages;
                let rc_page = self.pager.page_for_write(left_page_index);
                let mut left_page = rc_page.borrow_mut();
                left_page.init_as_leaf_page(false);
                left_page.wrap_slice(CELL_OFFSET, &buf);
                left_page.set_num_cells(FIRST_HALF_NUM_CELLS as u32);
                left_page.set_next_page(left_page_index + 1);
                left_page.set_parent_page_index(page_index);
                (page_index, left_page_index)
            }
        };

        // create a splitted page, and copy second half of page data into it
        let right_page_index = self.pager.num_pages;
        {
            let rc_page = self.pager.page_for_write(right_page_index);
            let mut right_page = rc_page.borrow_mut();
            right_page.init_as_leaf_page(false);
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
        let rc_page = self.pager.page_for_write(page_index);
        let mut page = rc_page.borrow_mut();
        page.set_key_for_cell(cell_index, key);
        let num_cells = page.get_num_cells();
        page.set_num_cells((num_cells + 1) as u32);
        // println!("write {}", key);
    }

    // this method is designed for dev or test purpose only.
    pub fn debug_print(&self) {
        println!("Tree:");
        if self.pager.num_pages > 0 {
            self.debug_print_for_page(0, "");
        }
    }

    fn debug_print_for_page(&self, page_index: usize, padding: &str) {
        let rc_page = self.pager.page_for_read(page_index);
        let page = rc_page.borrow();
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
}

impl BTreeTrait for BTree {
    fn search_key(&self, key: u32) -> CellIndex {
        if self.pager.num_pages == 0 {
            CellIndex::new(0, 0)
        } else {
            self.search_key_in_page(key, self.root_page_index)
        }
    }

    fn insert_key(&mut self, key: u32) -> Result<CellIndex, String> {
        // create page first.
        if self.pager.num_pages == 0 {
            let rc_page = self.pager.page_for_write(self.root_page_index);
            let mut first_page = rc_page.borrow_mut();
            first_page.init_as_leaf_page(true);
        }

        let CellIndex {
            page_index,
            cell_index,
        } = self.search_key(key);
        let num_cells = {
            let rc_page = self.pager.page_for_read(page_index);
            let page = rc_page.borrow();
            page.get_num_cells() as usize
        };

        if num_cells >= LEAF_NODE_MAX_CELLS {
            // split page
            self.split_leaf_page(page_index);
            return self.insert_key(key);
        } else if cell_index < num_cells {
            let rc_page = self.pager.page_for_write(page_index);
            let mut page = rc_page.borrow_mut();
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

impl BTreePage for Page {
    fn init_as_leaf_page(&mut self, is_root: bool) {
        self.set_page_type(PageType::Leaf);
        self.set_num_cells(0);
        self.set_is_root(is_root);
    }

    fn init_as_internal_page(&mut self, is_root: bool) {
        self.set_page_type(PageType::Internal);
        self.set_num_cells(0);
        self.set_is_root(is_root);
    }

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
