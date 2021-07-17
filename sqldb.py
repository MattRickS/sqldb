import collections
import enum
import functools
import math
import os
import sqlite3


COMPARISON_MAP = {
    "eq": "=",
    "ne": "!=",
    "in": "IN",
    "not_in": "NOT IN",
    "like": "LIKE",
    "unlike": "NOT LIKE",
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">=",
}


class Order(enum.Enum):
    Ascending = "ASC"
    Descending = "DESC"


class InvalidSchema(Exception):
    pass


def filters_to_query(filters):
    if not filters:
        return "", []

    values = []
    filter_list = []
    for filter_dict in filters:
        filter_option = []
        for comparison, data in filter_dict.items():
            key = COMPARISON_MAP[comparison]
            for field, value in data.items():
                if comparison == "in" or comparison == "not_in":
                    placeholder = "({})".format(",".join("?" for _ in value))
                    values.extend(value)
                elif value is None:
                    placeholder = "NULL"
                else:
                    placeholder = "?"
                    values.append(value)
                filter_option.extend([field, key, placeholder, "AND"])

        # Remove the last "AND" from the filters before joining
        if filter_option:
            filter_option.pop(-1)
            filter_list.append("({})".format(" ".join(filter_option)))

    filter_string = " OR ".join(filter_list)
    # Ensure the result is bracketed to prevent issues with multiple filter strings
    return (f"({filter_string})" if filter_string else ""), values


def order_to_query(order):
    return (
        " ".join(
            [
                "ORDER BY",
                ",".join(" ".join([field, direction]) for field, direction in order),
            ]
        )
        if order
        else ""
    )


class SQLiteDatabase(object):
    def __init__(self, database, sqlfile=None, id_field="id", log_callback=None):
        """
        Convenience wrapper for an sqlite database.

        Provides CRUD methods (one and many) for tables using a custom filter
        syntax and schema validation to prevent injection attacks. The
        transaction method can be used as a context manager so that all methods
        called within the context are placed into a transaction, eg

            db = SQLiteDatabase(":memory:")
            # Transaction start
            with db.transaction():
                db.create("table1", {"name": "one"})
                db.create("table1", {"name": "two"})
            # Transaction end


        Args:
            database (str): sqlite database to use, matches the `sqlite3.connect`
                argument of the same name.

        Keyword Args:
            sqlfile (str): Path to an sql file to load as the schema. Only used
                if the database file does not already exist.
            id_field (str): Name used for the id field on all tables.
            log_callback (Callable): A callable to be called for every database
                call, eg, logging.debug
        """
        self._filepath = database
        self._id_field = id_field

        exists = os.path.exists(self._filepath)
        self._connection = sqlite3.connect(
            self._filepath,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        self._connection.row_factory = sqlite3.Row

        if log_callback is not None:
            self._connection.set_trace_callback(log_callback)

        # If this is the first time the file is created, load the schema
        if not exists and sqlfile:
            with open(sqlfile) as f:
                self._initialise(f.read())

    @property
    def filepath(self):
        return self._filepath

    def __del__(self):
        self._connection.close()

    def _initialise(self, sql):
        with self._connection:
            self._connection.executescript(sql)

    def _execute(self, query, values=(), many=False):
        method = self._connection.executemany if many else self._connection.execute
        if self._connection.in_transaction:
            cursor = method(query, values)
        else:
            with self._connection:
                cursor = method(query, values)
        return cursor

    def __enter__(self):
        self._connection.execute("BEGIN IMMEDIATE")

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is None:
            self._connection.execute("COMMIT")
        else:
            self._connection.execute("ROLLBACK")

    def transaction(self):
        """ Returns a context manager where all methods become a single transaction """
        return self

    # ======================================================================== #
    # CRUD
    # ======================================================================== #

    def _get_max_pages(self, table, filter_string, values, limit):
        # TODO: Replace with page token
        count_sql = ("SELECT count(*) from", table, filter_string)
        query = " ".join(count_sql)
        cursor = self._connection.execute(query, values)
        num_rows = cursor.fetchone()[0]
        max_pages = int(math.ceil(num_rows / float(max(1, limit))))
        return max_pages

    def _fields(self, table, fields):
        if fields is None or "*" in fields:
            self._validate_table(table)
            fields = list(self.schema(table))
        else:
            fields.append(self._id_field)
            self._validate_fields(table, fields)

        fields = [f"{table}.{f}" for f in fields]
        fields.append(f"'{table}' as type")
        return fields

    def _build_joins(self, table_path, joins, query_fields):
        filter_strings = []
        filter_values = []
        join_strings = []

        for join_data in joins:
            prev_table = table_path[-1]
            table = join_data["table"]
            src_field = join_data.get("src_field", self._id_field)
            dst_field = prev_table + "." + join_data.get("dst_field", f"{table}_id")
            join_strings.append(f"JOIN {table} ON {table}.{src_field} = {dst_field}")

            conditions = join_data.get("conditions", [])
            filter_string, values = filters_to_query(conditions)
            if filter_string:
                filter_strings.append(filter_string)
                filter_values.extend(values)

            path = table_path + (table,)
            query_fields[path] = self._fields(table, join_data.get("fields"))
            # The joined field is removed - this is assuming it's an id field, may
            # not be desirable.
            try:
                query_fields[table_path].remove(dst_field)
            except ValueError:
                pass

            subjoins = join_data.get("joins")
            if subjoins:
                sub_join, sub_filters, sub_values = self._build_joins(
                    path, subjoins, query_fields
                )
                join_strings.extend(sub_join)
                if sub_filters:
                    filter_strings.extend(sub_filters)
                    filter_values.extend(sub_values)

        return join_strings, filter_strings, filter_values

    def _row(self, row, query_fields):
        i = 0
        result = None
        for table_path, fields in query_fields.items():
            # Last field is always: 'table' as type
            fields = [f.rsplit(".", 1)[-1] for f in fields[:-1]]
            fields.append("type")
            j = i + len(fields)

            values = dict(zip(fields, row[i:j]))
            i = j

            if result is None:
                result = values
            else:
                # First table is the root, and can be ignored
                temp = result
                for table in table_path[1:-1]:
                    temp = temp[table]
                temp[table_path[-1]] = values

        return result

    def _get(
        self,
        table,
        filters=None,
        fields=None,
        joins=None,
        order=None,
        limit=0,
        page=0,
    ):
        table_path = (table,)
        query_fields = collections.OrderedDict()
        query_fields[table_path] = self._fields(table, fields)

        sql = ["SELECT", "FROM", table]
        filter_string, values = filters_to_query(filters or [])

        if joins:
            join_strings, join_filters, join_values = self._build_joins(
                table_path, joins, query_fields
            )
            sql.extend(join_strings)
            if join_filters:
                filter_string += " AND ".join(join_filters)
                values.extend(join_values)

        sql.insert(
            1,
            ",".join(f for fields in query_fields.values() for f in fields),
        )

        if filter_string:
            sql.extend(("WHERE", filter_string))

        if order:
            sql.append(order_to_query(order))

        if limit:
            # If paginating the results, query the total number of rows that
            # match the filter criteria to determine the maximum number of pages
            max_pages = self._get_max_pages(table, filter_string, values, limit)

            # Only extend the values after the total rows query
            sql.extend(["LIMIT", "?,?"])
            values.extend([page * limit, limit])
        else:
            # If not paginating, every row is returned and the max_pages does
            # not need to be calculated
            max_pages = -1

        query = " ".join(sql) + ";"
        cursor = self._connection.execute(query, values)

        return cursor, query_fields, max_pages

    def create(self, table, fields):
        """
        Args:
            table (str): Name of a database table
            fields (dict): Dictionary of field, value pairs to store in the table

        Returns:
            int: ID of the created row
        """
        self._validate_fields(table, fields)

        fields, values = zip(*fields.items())
        query = "INSERT INTO {} ({}) VALUES ({});".format(
            table, ",".join(fields), ",".join("?" for _ in fields)
        )
        cursor = self._execute(query, values)

        return cursor.lastrowid

    def createmany(self, table, fields_list):
        """
        Creates multiple entries in one call, but does not return the uids

        Args:
            table (str): Name of a database table
            fields_list (list[dict]): List of field, value dictionaries
        """
        all_fields = {field for fields in fields_list for field in fields}
        self._validate_fields(table, all_fields)

        columns = list(all_fields)
        values = [tuple(fields.get(col) for col in columns) for fields in fields_list]
        query = "INSERT INTO {} ({}) VALUES ({});".format(
            table,
            ",".join(columns),
            ",".join("?" for _ in columns),
        )
        self._execute(query, values, many=True)

    def delete(self, table, uid):
        """
        Args:
            table (str): Name of a database table
            uid (int): Unique identifier for the row to delete

        Returns:
            bool: Whether or not the operation was successful
        """
        self._validate_table(table)
        query = " ".join(("DELETE FROM", table, "WHERE", self._id_field, "= ?;"))
        self._execute(query, (uid,))
        return True

    def deletemany(self, table, uids):
        """
        Args:
            table (str): Name of a database table
            uids (list[int]): List of unique identifiers to be deleted

        Returns:
            bool: Whether or not the operation was successful
        """
        self._validate_table(table)
        query = " ".join(("DELETE FROM", table, "WHERE", self._id_field, "= ?;"))
        self._execute(query, [(uid,) for uid in uids], many=True)
        return True

    def get(
        self, table, filters=None, fields=None, joins=None, order=None, limit=0, page=0
    ):
        """
        Args:
            table (str): Name of a database table

        Keyword Args:
            filters (list[dict]): Filters to reduce the number of rows queried
            fields (list[str]): List of fields to limit the returned data to.
                Must be fields that exist on the table. Default behaviour
                is decided by the driver, but a minimum of the ID field is returned.
            order (list[tuple[str, str]]): List of tuples for (field, Order)
            limit (int): Maximum number of rows to be returned. If 0 (default),
                all items are returned
            page (int): Page number to start querying from.

        Returns:
            tuple[int, list[dict]]: Tuple of (
                Maximum number of pages matching query or -1 if not paginated,
                List of field dictionaries matching the filters,
            )
        """
        cursor, query_fields, max_pages = self._get(
            table,
            filters=filters,
            fields=fields,
            joins=joins,
            order=order,
            limit=limit,
            page=page,
        )
        rows = cursor.fetchall()
        dict_rows = [self._row(row, query_fields) for row in rows]
        return max_pages, dict_rows

    def get_one(self, table, filters=None, fields=None, joins=None, order=None):
        """
        Args:
            table (str): Name of a database table

        Keyword Args:
            filters (list[dict]): Filters to reduce the number of rows queried
            fields (list[str]): List of fields to limit the returned data to.
                Must be fields that exist on the table. Default behaviour
                is decided by the driver, but a minimum of the ID field is returned.
            order (list[tuple[str, str]]): List of tuples for (field, Order)

        Returns:
            dict: A single database entry's fields
        """
        cursor, query_fields, _ = self._get(
            table, filters=filters, fields=fields, joins=joins, order=order
        )
        row = cursor.fetchone()
        if not row:
            return {}

        dict_row = self._row(row, query_fields)
        return dict_row

    def get_unique(self, table, fields, filters=None, order=None):
        """
        Args:
            table (str): Name of a database table
            fields (list[str]): Field names on the table

        Keyword Args:
            filters (list[dict]): Filters to reduce the number of rows queried
            order (list[tuple[str, str]]): List of tuples for (field, Order)
        Returns:
            list[dict]: List of unique combinations of values for the fields
                from the rows matching the filters
        """
        self._validate_fields(table, fields)

        sql = [
            "SELECT DISTINCT",
            ",".join(fields),
            "FROM",
            table,
            order_to_query(order),
        ]
        filter_string, values = filters_to_query(filters or [])
        if filter_string:
            sql[-2:-2] = ["WHERE", filter_string]

        query = " ".join(sql)
        cursor = self._connection.execute(query, values)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    @functools.lru_cache()
    def schema(self, table):
        """
        Args:
            table (str): Name of a database table

        Returns:
            dict[str, dict]: Dictionary of fields on the table mapped to data
                about the field, with a minimum of a "type" key mapped to the
                name of the data type it expects for the value

        Examples:
            >>> driver.schema("project")
            {
                "name": {"type": "string"},
                "id": {"type": "integer"},
            }
        """
        self._validate_table(table)

        cur = self._connection.cursor()
        columns = cur.execute(f"PRAGMA table_info('{table}')").fetchall()
        schema = {row[1]: {"type": row[2]} for row in columns}
        return schema

    def update(self, table, uid, fields):
        """
        Args:
            table (str): Name of a database table
            uid (int): Unique identifier for the row to update
            fields (dict): Dictionary of field, value pairs to update

        Returns:
            bool: Whether or not the operation was successful
        """
        self._validate_fields(table, fields)

        columns, values = zip(*fields.items())
        sql = [
            "UPDATE",
            table,
            "SET",
            ",".join(" ".join((col, "=", "?")) for col in columns),
            "WHERE",
            self._id_field,
            "= ?;",
        ]
        query = " ".join(sql)
        self._execute(query, values + (uid,))
        return True

    def updatemany(self, table, fields_list):
        """
        Args:
            table (str): Name of a database table
            fields_list (list[dict]): List of field, value pair dictionaries to
                update. All dictionaries must include the id field, and must be
                updating the same fields.

        Returns:
            bool: Whether or not the operation was successful
        """
        columns = set(fields_list[0])
        assert all(set(f) == columns for f in fields_list[1:])

        # Make sure the id field is validated before discarding from updates
        self._validate_fields(table, columns)
        columns.discard(self._id_field)

        sql = [
            "UPDATE",
            table,
            "SET",
            ",".join(" ".join((col, "=", f":{col}")) for col in columns),
            "WHERE",
            self._id_field,
            f"= :{self._id_field};",
        ]
        query = " ".join(sql)
        self._execute(query, fields_list, many=True)
        return True

    # ======================================================================== #
    # Utility
    # ======================================================================== #

    def _validate_fields(self, table, fields):
        schema = self.schema(table)
        invalid = set(fields).difference(schema)
        if invalid:
            raise InvalidSchema(f"Invalid field(s) for {table}: {list(invalid)}")

    def _validate_table(self, table):
        cur = self._connection.cursor()
        row = cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
            (table,),
        ).fetchone()
        if not row:
            raise InvalidSchema(f"Invalid table: {table}")
