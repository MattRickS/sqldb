# sqldb

Simple python wrapper for an sqlite database.

## Features

* Methods are designed to prevent SQL injection
* Table names and columns are validated to raise a consistent InvalidSchema exception
* Easy CRUD operations
* Simple filtering and ordering syntax
* Easy pagination
* Rows are returned as dictionaries with a minimum of "type" and "id"
* Queryable schema structure
* Convenience method `get_unique` for querying all unique values for a set of fields

## Filter syntax

Filters are handled as a list of dictionaries, where each dictionary defines criteria for matching rows. Each entry in the list is an "or" match, each entry in a dictionary is an "and". Dictionaries are comparison keys mapped to a dictionary of (field, value(s)) pairs.

To match all rows whose name is "John":  
`[{"eq": {"name": "John"}}]`

To match all rows whose name is "John" and who live in England or France:  
`[{"eq": {"name": "John"}, "in": {"country": ["England", "France"]}}]`
