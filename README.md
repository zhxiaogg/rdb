# rdb

[![Build Status](https://travis-ci.org/zhxiaogg/rdb.svg?branch=master)](https://travis-ci.org/zhxiaogg/rdb)

see [https://cstack.github.io/db_tutorial](https://cstack.github.io/db_tutorial).

## components and TODOs

There is a detailed explaination about [SQLite's architecture](http://www.sqlite.org/arch.html), this project aims to make a clone of that.

- pager (in progress)
  - [x] hashmap based implementation
  - [ ] use lru cache instead of hashmap
  - [ ] support mutliple tables in a single database file
  - [x] parameterized page size
- b+tree (for table, in progress)
  - [x] insertion of cells
  - [x] split of leaf node
  - [x] update parent node after leaf node split
  - [x] split of internal node
  - [ ] removal of cells
  - [ ] support arbitrary table schema
  - [ ] page structure needs to be revised
- vm
  - [x] a simple arithmetic vm
  - parser (in progress)
    - [ ] select
    - [ ] other statements
  - code gen (in progress)
  - sql execution plan
- b-tree (for index)
- transaction support
