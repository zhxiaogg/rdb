use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Index, IndexMut, Range, RangeFrom};

use byteorder::{BigEndian, ByteOrder};

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

// for leaf page layout:
const NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const NUM_CELLS_SIZE: usize = 4;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + NUM_CELLS_SIZE;

const CELL_OFFSET: usize = LEAF_NODE_HEADER_SIZE;
pub const KEY_SIZE: usize = 4;
const CELL_VALUE_SIZE: usize = ROW_SIZE;
pub const LEAF_NODE_CELL_SIZE: usize = KEY_SIZE + CELL_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// for internal page layout:
const NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const NUM_KEYS_SIZE: usize = 4;
const RIGH_PAGE_INDEX_OFFSET: usize = NUM_KEYS_OFFSET + NUM_KEYS_SIZE;
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
const CELL_END_OFFSET: usize = CELL_OFFSET + LEAF_NODE_MAX_CELLS * LEAF_NODE_CELL_SIZE;

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

pub type Page = Vec<u8>;

pub trait PageTrait {
    fn page_type(&self) -> PageType;

    fn set_page_type(&mut self, page_type: PageType);

    fn move_slice_internally(&mut self, from: usize, to: usize, len: usize);

    fn wrap_slice(&mut self, from: usize, buf: &Vec<u8>);

    fn set_parent_page_index(&mut self, page_index: usize);

    fn get_parent_page_index(&self) -> usize;

    fn is_root(&self) -> bool;

    fn set_is_root(&mut self, is_root: bool);
}

//TODO: update num cells using `usize`
pub trait LeafPage {
    fn new_leaf_page(is_root: bool) -> Page;

    fn pos_for_cell(cell_index: usize) -> usize;

    fn num_cells(&self) -> u32;

    fn key_for_cell(&self, cell_index: usize) -> u32;

    fn set_num_cells(&mut self, num_cells: u32);

    fn cell_for_key(&self, key: u32) -> usize;
}

pub trait InternalPage {
    fn set_num_keys(&mut self, num: usize);

    fn num_keys(&self) -> usize;

    fn set_key(&mut self, index: usize, key: u32);

    fn get_key(&self, index: usize) -> u32;

    fn set_page_index(&mut self, index: usize, page_index: usize);

    fn get_page_index(&self, index: usize) -> usize;

    // fn index_by_key(&self, key: u32) -> usize;
}

impl PageTrait for Page {
    fn page_type(&self) -> PageType {
        let v = self[PAGE_TYPE_OFFSET];
        PageType::from(v)
    }

    fn set_page_type(&mut self, page_type: PageType) {
        self[PAGE_TYPE_OFFSET] = page_type as u8;
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

    fn set_parent_page_index(&mut self, page_index: usize) {
        BigEndian::write_u32(
            self.index_mut(RangeFrom {
                start: PARENT_POINTER_OFFSET,
            }),
            page_index as u32,
        );
    }

    fn get_parent_page_index(&self) -> usize {
        BigEndian::read_u32(self.index(RangeFrom {
            start: PARENT_POINTER_OFFSET,
        })) as usize
    }

    fn is_root(&self) -> bool {
        self[IS_ROOT_OFFSET] == 1
    }

    fn set_is_root(&mut self, is_root: bool) {
        self[IS_ROOT_OFFSET] = if is_root { 1u8 } else { 0u8 };
    }
}

impl LeafPage for Page {
    fn new_leaf_page(is_root: bool) -> Page {
        let mut page = vec![0; PAGE_SIZE];
        page.set_page_type(PageType::Leaf);
        page.set_num_cells(0);
        page.set_is_root(is_root);
        page
    }

    fn pos_for_cell(cell_index: usize) -> usize {
        CELL_OFFSET + cell_index * LEAF_NODE_CELL_SIZE
    }

    fn num_cells(self: &Page) -> u32 {
        BigEndian::read_u32(self.index(RangeFrom {
            start: NUM_CELLS_OFFSET,
        }))
    }

    fn key_for_cell(self: &Page, cell_index: usize) -> u32 {
        if cell_index >= self.num_cells() as usize {
            panic!("invalid cell index!");
        }
        let pos = Page::pos_for_cell(cell_index);
        BigEndian::read_u32(self.index(RangeFrom { start: pos }))
    }

    fn set_num_cells(&mut self, num_cells: u32) {
        if (num_cells as usize) > LEAF_NODE_MAX_CELLS {
            panic!("max number of cells exceeded!");
        }
        BigEndian::write_u32(
            self.index_mut(RangeFrom {
                start: NUM_CELLS_OFFSET,
            }),
            num_cells,
        )
    }

    fn cell_for_key(&self, key: u32) -> usize {
        let num_cells = self.num_cells();
        if num_cells == 0 {
            return 0;
        }

        // binary search
        let mut high = num_cells as usize;
        let mut index = 0usize;
        while index != high {
            let mid = (index + high) / 2;
            let curr_key = self.key_for_cell(mid);
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

impl InternalPage for Page {
    fn set_num_keys(&mut self, num: usize) {
        BigEndian::write_u32(
            self.index_mut(RangeFrom {
                start: NUM_KEYS_OFFSET,
            }),
            num as u32,
        );
    }

    fn num_keys(&self) -> usize {
        BigEndian::read_u32(self.index(RangeFrom {
            start: NUM_KEYS_OFFSET,
        })) as usize
    }

    fn set_key(&mut self, index: usize, key: u32) {
        BigEndian::write_u32(
            self.index_mut(RangeFrom {
                start: KEY_INDEX_OFFSET + INDEX_SIZE + index * INTERNAL_NODE_CELL_SIZE,
            }),
            key,
        );
    }

    fn get_key(&self, index: usize) -> u32 {
        BigEndian::read_u32(self.index(RangeFrom {
            start: KEY_INDEX_OFFSET + INDEX_SIZE + index * INTERNAL_NODE_CELL_SIZE,
        }))
    }

    fn set_page_index(&mut self, index: usize, page_index: usize) {
        BigEndian::write_u32(
            self.index_mut(RangeFrom {
                start: KEY_INDEX_OFFSET + index * INTERNAL_NODE_CELL_SIZE,
            }),
            page_index as u32,
        )
    }

    fn get_page_index(&self, index: usize) -> usize {
        BigEndian::read_u32(self.index(RangeFrom {
            start: KEY_INDEX_OFFSET + index * INTERNAL_NODE_CELL_SIZE,
        })) as usize
    }
}

pub struct Pager {
    file: File,
    pages: Vec<Option<Page>>,
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
            file: file,
            pages: pages,
            num_pages: num_pages,
            root_page_index: 0,
        }
    }

    pub fn root_page_index(&self) -> usize {
        self.root_page_index
    }

    pub fn find_cell(&mut self, key: u32) -> Result<(usize, usize), String> {
        let page_index = self.root_page_index;
        let page = self.page_for_read(page_index);
        let page_type = page.page_type();
        match page_type {
            PageType::Leaf => {
                let cell_index = page.cell_for_key(key);
                Result::Ok((page_index, cell_index))
            }
            PageType::Internal => {
                Result::Err("Need to implement searching an internal node".to_owned())
            }
        }
    }

    /**
     * this method will insert key and return the cell position for later row serialization.
     * the returned cell position may not be the same as the input one, due to the
     * b+tree leaf node splitting.
     **/
    pub fn insert_key(
        &mut self,
        key: u32,
        page_index: usize,
        cell_index: usize,
    ) -> Result<(usize, usize), String> {
        let num_cells = if self.num_pages == 0 {
            0
        } else {
            let page = self.page_for_read(page_index);
            page.num_cells() as usize
        };

        if num_cells >= LEAF_NODE_MAX_CELLS {
            // split page
            let (relocated_page_index, new_page_index) = self.split_leaf_page(page_index);
            let mut real_page_index = page_index;
            let mut real_cell_index = cell_index;

            if cell_index >= FIRST_HALF_NUM_CELLS {
                real_page_index = new_page_index;
                real_cell_index = cell_index - FIRST_HALF_NUM_CELLS;
            } else if let Some(page_index) = relocated_page_index {
                real_page_index = page_index;
            }
            return self.insert_key(key, real_page_index, real_cell_index);
        } else if cell_index < num_cells {
            let page = self.page_for_write(page_index);
            if page.key_for_cell(cell_index) == key {
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
        Result::Ok((page_index, cell_index))
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
    fn split_leaf_page(&mut self, page_index: usize) -> (Option<usize>, usize) {
        let mut second_half_buf = vec![0u8; SECOND_HALF_PAGE_SIZE];
        let mut first_half_buf: Option<Vec<u8>> = None;
        let mut relocated_page_index = None;
        {
            let num_pages = self.num_pages;
            let original_page = self.page_for_write(page_index);
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


                // transform original page into root (type of Internal) page
                let key = original_page.key_for_cell(FIRST_HALF_NUM_CELLS - 1);
                original_page.set_page_type(PageType::Internal);
                original_page.set_num_keys(1);
                original_page.set_key(0, key);
                original_page.set_page_index(0, num_pages);
                original_page.set_page_index(1, num_pages + 1);
            } // TODO: else, we should update/split it's parent and all ways up to root node
        }
        if let Some(buf) = first_half_buf {
            // relocate the original page to a new page
            // and copy first half of original page data into the new page
            let num_pages = self.num_pages;
            relocated_page_index = Some(num_pages);
            let relocated_page = self.page_for_write(num_pages);
            relocated_page.wrap_slice(CELL_OFFSET, &buf);
            relocated_page.set_num_cells(FIRST_HALF_NUM_CELLS as u32);
        }

        // copy second half of page data into new page
        let new_page_index = self.num_pages;
        let new_page = self.page_for_write(new_page_index);
        new_page.wrap_slice(CELL_OFFSET, &second_half_buf);
        new_page.set_num_cells(SECOND_HALF_NUM_CELLS as u32);
        (relocated_page_index, new_page_index)
    }

    fn write_key(&mut self, key: u32, page_index: usize, cell_index: usize) {
        let cell_pos = Page::pos_for_cell(cell_index);
        let page = self.page_for_write(page_index);
        let num_cells = page.num_cells();
        BigEndian::write_u32(page.index_mut(RangeFrom { start: cell_pos }), key);
        page.set_num_cells((num_cells + 1) as u32);
    }

    pub fn flush(self: &mut Pager, page_index: usize) {
        let offset = page_index * PAGE_SIZE;
        if let Some(page) = self.pages[page_index].as_ref() {
            self.file.seek(SeekFrom::Start(offset as u64)).unwrap();
            self.file.write_all(page.as_ref()).unwrap();
        }
    }

    fn load(self: &mut Pager, page_index: usize) {
        let offset = page_index * PAGE_SIZE;
        self.file.seek(SeekFrom::Start(offset as u64)).unwrap();
        let mut buf = vec![0; PAGE_SIZE];
        self.file.read(buf.as_mut_slice()).unwrap();
        self.pages[page_index] = Some(buf);
    }

    // TODO: retreiving of a readable page should not pass a mutable pager
    pub fn page_for_read(self: &mut Pager, page_index: usize) -> &Page {
        if page_index >= self.num_pages {
            panic!("read EOF");
        } else if let None = self.pages[page_index] {
            self.load(page_index);
        }
        self.pages[page_index].as_ref().unwrap()
    }

    pub fn page_for_write(self: &mut Pager, page_index: usize) -> &mut Page {
        if page_index > self.num_pages {
            panic!("skipped write to a page");
        } else if page_index == self.num_pages {
            // need a new page
            self.pages[page_index] = Some(Page::new_leaf_page(page_index == 0));
            self.num_pages += 1;
        } else if let None = self.pages[page_index] {
            // load page from file
            self.load(page_index);
        }
        self.pages[page_index].as_mut().unwrap()
    }

    // this method is designed for dev or test purpose only.
    pub fn debug_print(&mut self) {
        println!("Tree:");
        if self.num_pages > 0 {
            self.debug_print_for_page(0, "");
        }
    }

    fn debug_print_for_page(&mut self, page_index: usize, padding: &str) {
        //TODO: when page_for_read get rid of mutable ref, we should refactor the following codes.
        match self.page_for_read(page_index).page_type() {
            PageType::Leaf => {
                let num_cells = self.page_for_read(page_index).num_cells();
                println!("{}- leaf (size {})", padding, num_cells);
                for cell_index in 0..(num_cells as usize) {
                    println!(
                        "{}  - {}",
                        padding,
                        self.page_for_read(page_index).key_for_cell(cell_index)
                    );
                }
            }
            PageType::Internal => {
                let num_keys = self.page_for_read(page_index).num_keys();
                println!("{}- internal (size {})", padding, num_keys);
                for index in 0..num_keys {
                    let child_page_index = self.page_for_read(page_index).get_page_index(index);
                    let key = self.page_for_read(page_index).get_key(index);

                    self.debug_print_for_page(child_page_index, &format!("{}  ", padding));

                    println!("{}- key {}", padding, key);
                }
                let child_page_index = self.page_for_read(page_index).get_page_index(num_keys);
                self.debug_print_for_page(child_page_index, &format!("{}  ", padding));
            }
        }
    }
}
