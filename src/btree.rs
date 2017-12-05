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

pub trait BTree {
    fn search_key(&self, key: u32) -> CellIndex;

    /**
     * this method will insert key and return the inserted cell index.
     **/
    fn insert_key(&mut self, key: u32) -> Result<CellIndex, String>;
}

pub enum PageType {
    Internal = 0,
    Leaf = 1,
}

pub trait BTreePage {
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
