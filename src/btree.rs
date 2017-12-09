use pager::{Page, PageTrait, Pager};
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

// for internal page layout:
const RIGH_PAGE_INDEX_OFFSET: usize = NUM_CELLS_OFFSET + NUM_CELLS_SIZE;
const RIGHT_PAGE_INDEX_SIZE: usize = 4;

const INTERNAL_NODE_HEADER_SIZE: usize = RIGH_PAGE_INDEX_OFFSET + RIGHT_PAGE_INDEX_SIZE;
const KEY_INDEX_OFFSET: usize = INTERNAL_NODE_HEADER_SIZE;
const INDEX_SIZE: usize = 4;
const INTERNAL_NODE_CELL_SIZE: usize = INDEX_SIZE + KEY_SIZE;

pub struct BTreeConfig {
    page_size: usize,
}

impl BTreeConfig {
    pub fn new(page_size: usize) -> BTreeConfig {
        BTreeConfig {
            page_size: page_size,
        }
    }

    pub fn print_constants(&self) {
        println!("Constants:");
        println!("PAGE_SIZE: {}", self.page_size);
        println!("ROW_SIZE: {}", ROW_SIZE);
        println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
        println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
        println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
        println!(
            "LEAF_NODE_SPACE_FOR_CELLS: {}",
            self.page_size - LEAF_NODE_HEADER_SIZE
        );
        println!("LEAF_NODE_MAX_CELLS: {}", self.get_max_num_cells_for_leaf());
        println!("INTERNAL_NODE_HEADER_SIZE: {}", INTERNAL_NODE_HEADER_SIZE);
        println!("INTERNAL_NODE_CELL_SIZE: {}", INTERNAL_NODE_CELL_SIZE);
    }

    // pub fn get_page_size(&self) -> usize{
    //     self.page_size
    // }

    pub fn get_max_num_cells_for_leaf(&self) -> usize {
        (self.page_size - LEAF_NODE_HEADER_SIZE) / LEAF_NODE_CELL_SIZE
    }

    pub fn get_max_num_cells_for_internal(&self) -> usize {
        (self.page_size - INTERNAL_NODE_HEADER_SIZE - RIGHT_PAGE_INDEX_SIZE)
            / INTERNAL_NODE_CELL_SIZE
    }
}



pub trait BTreeTrait {
    fn search_key(&self, key: u32) -> CellIndex;

    /**
     * this method will insert key and return the inserted cell index.
     **/
    fn insert_key(&mut self, key: u32) -> Result<CellIndex, String>;
}

pub trait BTreePage {
    fn init_as_leaf_page(&mut self, is_root: bool, num_cells: u32);

    fn init_as_internal_page(&mut self, is_root: bool, num_cells: u32);

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

struct SplitHelper<'a> {
    original: &'a mut Page,
    right_page: &'a mut Page,
    left_page: Option<&'a mut Page>,
    pager: &'a mut Pager,
    split_position: usize,
    original_index: usize,
    right_page_index: usize,
    left_page_index: usize,
}

impl<'a> SplitHelper<'a> {
    fn new(
        original_page: &'a mut Page,
        original_page_index: usize,
        left_page: Option<&'a mut Page>,
        left_page_index: usize,
        right_page: &'a mut Page,
        right_page_index: usize,
        split_position: usize,
        pager: &'a mut Pager,
    ) -> SplitHelper<'a> {
        SplitHelper {
            original: original_page,
            original_index: original_page_index,
            left_page: left_page,
            right_page: right_page,
            left_page_index: left_page_index,
            right_page_index: right_page_index,
            split_position: split_position,
            pager: pager,
        }
    }

    fn get_page(&mut self, cell_index: usize) -> &mut Page {
        match cell_index > self.split_position {
            true => self.right_page,
            false => match self.left_page {
                Some(ref mut page) => page,
                None => self.original,
            },
        }
    }

    fn get_page_index(&self, cell_index: usize) -> usize {
        match cell_index > self.split_position {
            true => self.right_page_index,
            false => self.left_page_index,
        }
    }

    fn translate_cell_index(&self, cell_index: usize) -> usize {
        match cell_index > self.split_position {
            true => cell_index - self.split_position - 1,
            false => cell_index,
        }
    }

    fn set_internal_cell(&mut self, cell_index: usize, page_index: usize, key: Option<u32>) {
        let real_cell_index = self.translate_cell_index(cell_index);
        let real_page_index = self.get_page_index(cell_index);
        {
            let page = self.get_page(cell_index);
            if let Some(k) = key {
                page.set_key_for_cell(real_cell_index, k);
            }
            page.set_page_index(real_cell_index, page_index);
        }
        // update parent page index
        let rc_page = self.pager.page_for_write(page_index);
        match rc_page.try_borrow_mut() {
            Result::Ok(mut page) => page.set_parent_page_index(real_page_index),
            Result::Err(_) => panic!("cannot borrow page {}", page_index),
        };
    }

    fn get_internal_page_split_result(&self) -> (usize, u32, usize, usize) {
        let (parent_page_index, left_max_key) = match self.left_page {
            Some(ref page) => (
                self.original_index,
                page.get_key_for_cell(self.split_position),
            ),
            None => (
                self.original.get_parent_page_index(),
                self.original.get_key_for_cell(self.split_position),
            ),
        };

        (
            parent_page_index,
            left_max_key,
            self.left_page_index,
            self.right_page_index,
        )
    }

    /**
     * original node: N = 4, M = 2: A;4|B;7|C;9|D;11|E
     * insert (X;8;Y), N=5: A;4|B;7|X;8|Y;9|D;11|E
     * indeces:               0|  1|  2|  3|   4|5
     * indeces of 3, 4, 5 will be moved to right page;
     * indeces of 0, 1, 2 will be moved to left page;
     *
     * returns split result (parent_page_index, new_key, left_page_index, right_page_index)
     *
     **/
    fn split_internal_page(
        &mut self,
        key: u32,
        left_page_index: usize,
        right_page_index: usize,
    ) -> (usize, u32, usize, usize) {
        let num_cells = self.original.get_num_cells() as usize;
        let mut inserted = false;
        // let N = num_cells + 1;
        // here we choose moving by deserialize & serialize
        // however we could also choose moving bytes directly.
        for j in (0..num_cells + 1).rev() {
            let mut new_cell_index = match inserted {
                true => j,
                false => j + 1,
            };
            if !inserted && (j == 0 || key > self.original.get_key_for_cell(j - 1)) {
                // found insertion position
                if j < num_cells {
                    let k = self.original.get_key_for_cell(j);
                    self.set_internal_cell(new_cell_index, right_page_index, Some(k));
                } else {
                    self.set_internal_cell(new_cell_index, right_page_index, None);
                }

                let next_cell_index = new_cell_index - 1;
                self.set_internal_cell(next_cell_index, left_page_index, Some(key));

                inserted = true;
            } else {
                let original_page_index = self.original.get_page_index(j);
                if j < num_cells {
                    let k = self.original.get_key_for_cell(j);
                    self.set_internal_cell(new_cell_index, original_page_index, Some(k));
                } else {
                    self.set_internal_cell(new_cell_index, original_page_index, None);
                }
            }
        }

        // set page properties accordingly
        match self.left_page {
            Some(_) => self.original.init_as_internal_page(true, 0),
            None => self.original.set_num_cells(self.split_position as u32),
        };

        return self.get_internal_page_split_result();
    }
}

pub struct BTree {
    pub pager: Pager,
    root_page_index: usize,
    pub config: BTreeConfig,
}

impl BTree {
    pub fn new(pager: Pager) -> BTree {
        let config = BTreeConfig::new(pager.get_page_size());

        BTree {
            pager: pager,
            root_page_index: 0,
            config: config,
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
        let num_cells = {
            let rc_page = self.pager.page_for_read(page_index);
            let page = rc_page.borrow();
            page.get_num_cells() as usize
        };

        if num_cells >= self.config.get_max_num_cells_for_internal() {
            self.split_internal_page_and_insert_key(
                page_index,
                key,
                left_page_index,
                right_page_index,
            );
            return;
        }

        let rc_page = self.pager.page_for_write(page_index);
        let mut page = rc_page.borrow_mut();
        let cell_index = page.find_cell_for_key(key);
        if cell_index < num_cells {
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

    fn split_internal_page_and_insert_key(
        &mut self,
        page_index: usize,
        key: u32,
        left_page_index: usize,
        right_page_index: usize,
    ) {
        let internal_node_max_cells = self.config.get_max_num_cells_for_internal();
        let first_half_num_cells = (internal_node_max_cells + 1) / 2;
        let second_half_num_cells = internal_node_max_cells - first_half_num_cells;

        let (parent_page_index, max_left_key, new_left_page_index, new_right_page_index) = {
            let original_page_index = page_index;
            let rc_original_page = self.pager.page_for_write(original_page_index);
            let original_page = &mut rc_original_page.borrow_mut();

            let new_right_page_index = self.pager.next_page_index();
            let rc_new_right_page = self.pager.page_for_write(new_right_page_index);
            let new_right_page = &mut rc_new_right_page.borrow_mut();
            new_right_page.init_as_internal_page(false, second_half_num_cells as u32);

            let is_root = original_page.is_root();
            if is_root {
                let new_left_page_index = self.pager.next_page_index();
                let rc_new_left_page = self.pager.page_for_write(new_left_page_index);
                let new_left_page = &mut rc_new_left_page.borrow_mut();
                new_left_page.init_as_internal_page(false, first_half_num_cells as u32);

                new_left_page.set_parent_page_index(original_page_index);
                new_right_page.set_parent_page_index(original_page_index);

                let mut selector = SplitHelper::new(
                    original_page,
                    original_page_index,
                    Some(new_left_page),
                    new_left_page_index,
                    new_right_page,
                    new_right_page_index,
                    first_half_num_cells,
                    &mut self.pager,
                );
                selector.split_internal_page(key, left_page_index, right_page_index)
            } else {
                let parent_page_index = original_page.get_parent_page_index();
                new_right_page.set_parent_page_index(parent_page_index);

                let mut selector = SplitHelper::new(
                    original_page,
                    original_page_index,
                    None,
                    page_index,
                    new_right_page,
                    new_right_page_index,
                    first_half_num_cells,
                    &mut self.pager,
                );

                selector.split_internal_page(key, left_page_index, right_page_index)
            }
        };
        // update parent
        self.insert_key_into_internal(
            parent_page_index,
            max_left_key,
            new_left_page_index,
            new_right_page_index,
        );
    }

    /**
     * Split a leaf page identified by given page_index.
     * If the given page is root page, then two new page will be created, and the
     * original page will be emptied and relocated. Otherwise only one new page will
     * be created.
     * This method returns a optinal relocated page_index (if original page is root page) and
     * the newly created page index.
     * TODO: bytes move not efficient!
     * TODO: move to SplitHelper
     **/
    fn split_leaf_page(&mut self, page_index: usize) {
        let leaf_node_max_cells = self.config.get_max_num_cells_for_leaf();
        let first_half_num_cells = (leaf_node_max_cells + 1) / 2;
        let second_half_num_cells = leaf_node_max_cells - first_half_num_cells;
        let first_half_page_size = first_half_num_cells * LEAF_NODE_CELL_SIZE;
        let second_half_cells_offset = CELL_OFFSET + first_half_page_size;
        let second_half_page_size = second_half_num_cells * LEAF_NODE_CELL_SIZE;

        let mut second_half_buf = vec![0u8; second_half_page_size];
        let mut first_half_buf: Option<Vec<u8>> = None;
        let new_key;
        // copy bytes into vectors, which is inefficient
        //TODO: inefficient copy of bytes
        {
            let rc_page = self.pager.page_for_write(page_index);
            let mut original_page = rc_page.borrow_mut();
            new_key = original_page.get_key_for_cell(first_half_num_cells - 1);
            second_half_buf.clone_from_slice(original_page.index(Range {
                start: second_half_cells_offset,
                end: second_half_cells_offset + second_half_page_size,
            }));
            if original_page.is_root() {
                let mut buf = vec![0u8; first_half_page_size];
                buf.clone_from_slice(original_page.index(Range {
                    start: CELL_OFFSET,
                    end: CELL_OFFSET + first_half_page_size,
                }));
                first_half_buf = Some(buf);

                // reset original root page
                original_page.init_as_internal_page(true, 0);
            } else {
                original_page.set_num_cells(first_half_num_cells as u32);
                original_page.set_next_page(self.pager.next_page_index());
            }
        }

        // create a new leaf page if the original page is root
        let (parent_page_index, left_page_index, next_page_index) = match first_half_buf {
            None => {
                let rc_page = self.pager.page_for_read(page_index);
                let page = rc_page.borrow();
                (
                    page.get_parent_page_index(),
                    page_index,
                    page.get_next_page(),
                )
            }
            Some(buf) => {
                let left_page_index = self.pager.next_page_index();
                let rc_page = self.pager.page_for_write(left_page_index);
                let mut left_page = rc_page.borrow_mut();
                left_page.init_as_leaf_page(false, first_half_num_cells as u32);
                left_page.wrap_slice(CELL_OFFSET, &buf);
                left_page.set_next_page(left_page_index + 1);
                left_page.set_parent_page_index(page_index);
                (page_index, left_page_index, 0)
            }
        };

        // create a splitted page, and copy second half of page data into it
        let right_page_index = self.pager.next_page_index();
        {
            let rc_page = self.pager.page_for_write(right_page_index);
            let mut right_page = rc_page.borrow_mut();
            right_page.init_as_leaf_page(false, second_half_num_cells as u32);
            right_page.wrap_slice(CELL_OFFSET, &second_half_buf);
            right_page.set_next_page(next_page_index);
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
    }

    // this method is designed for dev or test purpose only.
    pub fn debug_print(&self, only_internal: bool) {
        println!("Tree:");
        if self.pager.num_pages > 0 {
            self.debug_print_page(0, "", only_internal);
        }
    }

    fn debug_print_page(&self, page_index: usize, padding: &str, only_internal: bool) {
        let rc_page = self.pager.page_for_read(page_index);
        let page = rc_page.borrow();
        match page.get_page_type() {
            PageType::Leaf => {
                if !only_internal {
                    let num_cells = page.get_num_cells() as usize;
                    println!("{}- leaf (size {})", padding, num_cells);
                    for cell_index in 0..num_cells {
                        println!("{}  - {}", padding, page.get_key_for_cell(cell_index));
                    }
                }
            }
            PageType::Internal => {
                let num_keys = page.get_num_cells() as usize;
                println!("{}- internal (size {})", padding, num_keys);
                let new_padding = &format!("{}  ", padding);
                for index in 0..num_keys + 1 {
                    let child_index = page.get_page_index(index);
                    self.debug_print_page(child_index, new_padding, only_internal);
                    if !only_internal && index < num_keys {
                        let key = page.get_key_for_cell(index);
                        println!("{}- key {}", new_padding, key);
                    }
                }
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
            first_page.init_as_leaf_page(true, 0);
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

        if num_cells >= self.config.get_max_num_cells_for_leaf() {
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

fn range_for_internal_page_index(page_size: usize, index: usize) -> RangeFrom<usize> {
    let max_cells = (page_size - INTERNAL_NODE_HEADER_SIZE) / INTERNAL_NODE_CELL_SIZE;
    if index >= max_cells {
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
    fn init_as_leaf_page(&mut self, is_root: bool, num_cells: u32) {
        self.set_page_type(PageType::Leaf);
        self.set_num_cells(num_cells);
        self.set_is_root(is_root);
    }

    fn init_as_internal_page(&mut self, is_root: bool, num_cells: u32) {
        self.set_page_type(PageType::Internal);
        self.set_num_cells(num_cells);
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
        let page_size = self.len();
        BigEndian::write_u32(
            self.index_mut(range_for_internal_page_index(page_size, index)),
            page_index as u32,
        )
    }

    fn get_page_index(&self, index: usize) -> usize {
        BigEndian::read_u32(self.index(range_for_internal_page_index(self.len(), index))) as usize
    }

    fn find_page_for_key(&self, key: u32) -> usize {
        self.get_page_index(self.find_cell_for_key(key))
    }
}
