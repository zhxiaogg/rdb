# rdb

see [https://cstack.github.io/db_tutorial](https://cstack.github.io/db_tutorial).

## components and TODOs

There is a detailed explaination about [SQLite's architecture](http://www.sqlite.org/arch.html), this project aims to make a clone of that.

- pager (in progress)
  - [x] hashmap based implementation
  - [ ] use lru cache instead of hashmap
  - [ ] support mutliple tables in a single database file
- b+tree (for table, in progress)
  - [x] insertion of cells
  - [x] split of leaf node
  - [x] update parent node after leaf node split
  - [ ] split of internal node
  - [ ] removal of cells
  - [ ] support arbitrary table schema
  - [ ] page structure needs to be revised
- vm
  - parser
  - code gen
  - sql execution plan
- b-tree (for index)
- transaction support
