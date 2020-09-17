# sqldb

Simple python wrapper for an sqlite database.

## Features

* Methods are designed to prevent SQL injection
* Table names and columns are validated to raise a consistent InvalidSchema exception
* Easy CRUD operations
* Simple filtering and ordering syntax
* Easy pagination
* Rows are returned as dictionaries
* Queryable schema structure
* Convenience method `get_unique` for querying all unique values for a set of fields
