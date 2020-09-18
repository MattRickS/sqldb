import enum
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

    filter_sql = ["WHERE", " OR ".join(filter_list)]
    filter_string = " ".join(filter_sql)
    return filter_string, values


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


def get_max_pages(con, entity_type, filter_string, values, limit):
    count_sql = ["SELECT count(*) from", entity_type, filter_string]
    query = " ".join(count_sql)
    con.execute(query, values)
    num_rows = con.fetchone()[0]
    max_pages = int(math.ceil(num_rows / float(max(1, limit))))
    return max_pages


class SQLiteDatabase(object):
    ID_FIELD = "id"

    def __init__(self, dbfile):
        self._filepath = dbfile
        exists = os.path.exists(self._filepath)
        self._connection = sqlite3.connect(
            self._filepath,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            # TODO: Test out the various levels, autocommit might be useful
            isolation_level="IMMEDIATE",
        )
        self._connection.row_factory = sqlite3.Row

        # If this is the first time the file is created, load the schema
        if not exists:
            self._initialise()

    def _initialise(self):
        pass

    def cursor(self):
        return self._connection.cursor()

    # ======================================================================== #
    # Entities
    # ======================================================================== #

    def _get(
        self,
        connection,
        entity_type,
        filters=None,
        fields=None,
        order=None,
        limit=0,
        page=0,
    ):
        if not fields:
            fields = ["*"]
            self._validate_table(entity_type)
        else:
            fields.append(self.ID_FIELD)
            self._validate_fields(entity_type, fields)

        filter_string, values = filters_to_query(filters or [])
        sql = ["SELECT", ",".join(fields), "FROM", entity_type, filter_string]

        if order:
            sql.append(order_to_query(order))

        if limit:
            # If paginating the results, query the total number of rows that
            # match the filter criteria to determine the maximum number of pages
            max_pages = get_max_pages(
                connection, entity_type, filter_string, values, limit
            )

            # Only extend the values after the total rows query
            sql.extend(["LIMIT", "?,?"])
            values.extend([page * limit, limit])
        else:
            # If not paginating, every row is returned and the max_pages does
            # not need to be calculated
            max_pages = -1

        query = " ".join(sql) + ";"
        connection.execute(query, values)

        return max_pages

    def create(self, entity_type, fields):
        """
        Args:
            entity_type (str): Name of a database entity type
            fields (dict): Dictionary of field, value pairs to store in the entity

        Returns:
            int: ID of the created entity
        """
        self._validate_fields(entity_type, fields)

        fields, values = zip(*fields.items())
        con = self._connection.cursor()
        try:
            con.execute(
                "INSERT INTO {} ({}) VALUES ({});".format(
                    entity_type, ",".join(fields), ",".join("?" for _ in fields)
                ),
                values,
            )
        except Exception:
            self._connection.rollback()
            raise
        else:
            self._connection.commit()

        return con.lastrowid

    def delete(self, entity_type, uid):
        """
        Args:
            entity_type (str): Name of a database entity type
            uid (int): Unique identifier for the entity to delete

        Returns:
            bool: Whether or not the operation was successful
        """
        self._validate_table(entity_type)
        with self._connection:
            query = " ".join(
                ("DELETE FROM", entity_type, "WHERE", self.ID_FIELD, "= ?;")
            )
            self._connection.execute(query, (uid,))
        return True

    def get(self, entity_type, filters=None, fields=None, order=None, limit=0, page=0):
        """
        Args:
            entity_type (str): Name of a database entity type

        Keyword Args:
            filters (list[dict]): Filters to reduce the number of entities queried
            fields (list[str]): List of fields to limit the returned data to.
                Must be fields that exist on the entity type. Default behaviour
                is decided by the driver, but a minimum of the ID field is returned.
            order (list[tuple[str, str]]): List of tuples for (field, Order)
            limit (int): Maximum number of entities to be returned. If 0 (default),
                all items are returned
            page (int): Page number to start querying from.

        Returns:
            tuple[int, list[dict]]: Tuple of (
                Maximum number of pages matching query or -1 if not paginated,
                List of field dictionaries matching the filters,
            )
        """
        con = self._connection.cursor()
        max_pages = self._get(
            con,
            entity_type,
            filters=filters,
            fields=fields,
            order=order,
            limit=limit,
            page=page,
        )
        rows = con.fetchall()
        dict_rows = [dict(row) for row in rows]
        return max_pages, dict_rows

    def get_one(self, entity_type, filters=None, fields=None, order=None):
        """
        Args:
            entity_type (str): Name of a database entity type

        Keyword Args:
            filters (list[dict]): Filters to reduce the number of entities queried
            fields (list[str]): List of fields to limit the returned data to.
                Must be fields that exist on the entity type. Default behaviour
                is decided by the driver, but a minimum of the ID field is returned.
            order (list[tuple[str, str]]): List of tuples for (field, Order)

        Returns:
            dict: A single database entry's fields
        """
        con = self._connection.cursor()
        self._get(con, entity_type, filters=filters, fields=fields, order=order)
        row = con.fetchone()
        if not row:
            return {}

        dict_row = dict(row)
        return dict_row

    def get_unique(self, entity_type, fields, filters=None, order=None):
        """
        Args:
            entity_type (str): Name of a database entity type
            fields (list[str]): Field names on the entity type

        Keyword Args:
            filters (list[dict]): Filters to reduce the number of entities queried
            order (list[tuple[str, str]]): List of tuples for (field, Order)
        Returns:
            list[dict]: List of unique combinations of values for the fields
                from the entities matching the filters
        """
        self._validate_fields(entity_type, fields)

        filter_string, values = filters_to_query(filters or [])
        sql = [
            "SELECT DISTINCT",
            ",".join(fields),
            "FROM",
            entity_type,
            filter_string,
            order_to_query(order),
        ]
        query = " ".join(sql)
        con = self._connection.cursor()
        con.execute(query, values)
        rows = con.fetchall()
        return [dict(row) for row in rows]

    def schema(self, entity_type):
        """
        Args:
            entity_type (str): Name of a database entity type

        Returns:
            dict[str, dict]: Dictionary fields on the entity mapped to data
                about the field, with a minimum of a "type" key with the name of
                the class it expects for the value

        Examples:
            >>> driver.schema("Asset")
            {
                "name": {"type": "string"},
                "id": {"type": "integer"},
                "project": {"type": "entity"},
            }
        """
        self._validate_table(entity_type)

        cur = self._connection.cursor()
        columns = cur.execute("PRAGMA table_info('{}')".format(entity_type)).fetchall()
        schema = {row[1]: {"type": row[2]} for row in columns}
        return schema

    def update(self, entity_type, uid, fields):
        """
        Args:
            entity_type (str): Name of a database entity type
            uid (int): Unique identifier for the entity to update
            fields (dict): Dictionary of field, value pairs to update

        Returns:
            bool: Whether or not the operation was successful
        """
        self._validate_fields(entity_type, fields)

        columns, values = zip(*fields.items())
        sql = [
            "UPDATE",
            entity_type,
            "SET",
            ",".join(" ".join((col, "=", "?")) for col in columns),
            "WHERE",
            self.ID_FIELD,
            "= ?;",
        ]
        query = " ".join(sql)
        with self._connection:
            self._connection.execute(query, values + (uid,))
        return True

    # ======================================================================== #
    # Utility
    # ======================================================================== #

    def _validate_fields(self, entity_type, fields):
        schema = self.schema(entity_type)
        invalid = set(fields).difference(schema)
        if invalid:
            raise InvalidSchema(
                "Invalid field(s) for {}: {}".format(entity_type, list(invalid))
            )

    def _validate_table(self, entity_type):
        cur = self._connection.cursor()
        row = cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
            (entity_type,),
        ).fetchone()
        if not row:
            raise InvalidSchema("Invalid entity type: {}".format(entity_type))
