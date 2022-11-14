# File System Database

This repository is for the final project of **CMPE 321 Introduction to Database Systems** course.

As of the scope of this project, a file system is designed and implemented from scratch. The database that this file system corresponds is used for storing the data about the lore for the video game Diablo, namely Horadrim.

## Table of Contents
1. System Design
2. Indexing
3. Development
4. Details
5. How to Run

### System Design

The system design has a system catalog for storing the metadata and data storage units (files, pages, and records) for storing the actual data. The system design also supports following Definition Language Operations and Manipulation Language Operations:
* Definition Language Operations
1. Create a type
2. Delete a type
3. List all types

* Manipulation Language Operations
1. Create a record
2. Delete a record
3. Search for a record (by primary key) • Update a record (by primary key)
4. List all records of a type
5. Filter records (by primary key)

For the system design, there are also some instructions for system to work properly. These instructions are mentioned in the `constants.py` file.

There are also some assumptions for the project:
* All fields shall be alphanumeric. Also, type and field names shall be alphanumeric.
* User always enters alphanumeric characters inside the test cases.
* The hardware of Horadrim center and Horadrim instances will be built according to the blueprints, thus you do not need to consider the Horadrim physical storage controller to interact with the storage units.
* Note that type refers to a relation.

### Indexing

B+ tree is used for indexing. Primary keys of types are used for the search-keys in trees. There is a separate B+-Tree for each type, and these trees are be stored in file(s). So, when a type is created, its B+ Tree will also be created and its structure is be stored. With every create record and delete record operations, the corresponding tree is updated. B+ Trees are not generated from scratch on the fly using records for DML operations. Tree structures that are stored in files are utilized to serialize/deserialize during the operations. Each tree is written as a JSON file and is read before performing any operation.

### Development

For the development of the project, Python is used.

### Details

Any further detail is published in the project report, which can be reached here.

### How to Run

Before running make sure below specified conditions under the current directory are met:
* file_index.json file only has one field current_index and its value must be 1. Like below:
> {"current_index": 1}
* types.json file only has one field types and its value must be an empty list []. Like below:
> {"types": []}
* prim_keys.json file must contain an empty dict. Like below:
> {}

If above conditions are met, simply navigate to the src with:
> cd src

Then run:
> python3 horadrimSoftware.py inputFile.txt outputFile.txt

Note: It only creates at most 99 files specified in constants.py
