import sqlite3
import pytest

import sqldb


@pytest.fixture(scope="function")
def db():
    return sqldb.SQLiteDatabase(":memory:", log_callback=print)


def test_crud(db: sqldb.SQLiteDatabase):
    db._initialise(
        """
    create table project (
        id          integer primary key autoincrement not null,
        display     text not null,
        name        text not null UNIQUE,
        description text,
        created     timestamp default current_timestamp,
        creator     text not null
    );
    """
    )

    # Create
    uid = db.create(
        "project",
        {"name": "MyProject", "display": "My Project", "creator": "mshaw"},
    )
    assert uid == 1

    # Read one
    project = db.get_one(
        "project", filters=[{"eq": {"id": uid}}], fields=["display", "description"]
    )
    assert project == {
        "type": "project",
        "id": 1,
        "display": "My Project",
        "description": None,
    }

    # Updating
    result = db.update(
        "project", uid, {"description": "Demonstrating the python DB wrapper"}
    )
    assert result is True
    assert (
        db.get_one("project", fields=["description"])["description"]
        == "Demonstrating the python DB wrapper"
    )

    # Read many
    max_pages, projects = db.get("project", fields=["name", "creator"])
    assert max_pages == -1
    assert projects == [
        {"type": "project", "id": 1, "name": "MyProject", "creator": "mshaw"}
    ]

    # Deleting
    result = db.delete("project", uid)
    assert result is True
    assert db.get_one("project") == {}


def test_crudmany(db: sqldb.SQLiteDatabase):
    db._initialise(
        """
    create table project (
        id          integer primary key autoincrement not null,
        display     text not null,
        name        text not null UNIQUE,
        description text,
        created     timestamp default current_timestamp,
        creator     text not null
    );
    """
    )

    # Create
    db.createmany(
        "project",
        [
            {"name": "MyProject", "display": "My Project", "creator": "mshaw"},
            {"name": "OtherProject", "display": "Other Project", "creator": "mshaw"},
            {"name": "Something", "display": "Something", "creator": "mshaw"},
        ],
    )

    # Updating
    result = db.updatemany(
        "project",
        [
            {"id": 1, "description": "Demonstrating the python DB wrapper"},
            {"id": 2, "description": "Demonstrating the python DB wrapper"},
        ],
    )
    assert result is True

    # Read many
    max_pages, projects = db.get("project", fields=["name", "description"])
    assert max_pages == -1
    assert projects == [
        {
            "type": "project",
            "id": 1,
            "name": "MyProject",
            "description": "Demonstrating the python DB wrapper",
        },
        {
            "type": "project",
            "id": 2,
            "name": "OtherProject",
            "description": "Demonstrating the python DB wrapper",
        },
        {"type": "project", "id": 3, "name": "Something", "description": None},
    ]

    # Delete Many
    result = db.deletemany("project", [2, 3])
    assert result is True
    _, projects = db.get("project", fields=[])
    assert projects == [{"type": "project", "id": 1}]


def test_get_methods(db: sqldb.SQLiteDatabase):
    db._initialise(
        """
    create table project (
        id          integer primary key autoincrement not null,
        name        text not null UNIQUE
    );
    create table user (
        id          integer primary key autoincrement not null,
        name        text not null UNIQUE,
        age         integer not null
    );
    """
    )

    for i in range(20):
        db.create("project", {"name": "Project{}".format(i)})
        db.create("user", {"name": "User{}".format(i), "age": i % 3 + 20})

    # Get many with limit defaults to first page
    max_pages, entities = db.get("project", fields=["id"], limit=3)
    assert max_pages == 7  # 1 + 20 // 3
    assert entities == [
        {"id": 1, "type": "project"},
        {"id": 2, "type": "project"},
        {"id": 3, "type": "project"},
    ]

    # Page can be set
    max_pages, entities = db.get("project", fields=["id"], limit=3, page=1)
    assert max_pages == 7  # 1 + 20 // 3
    assert entities == [
        {"id": 4, "type": "project"},
        {"id": 5, "type": "project"},
        {"id": 6, "type": "project"},
    ]

    # Ordering can be specified
    _, entities = db.get("project", fields=["id"], limit=3, order=[("id", "DESC")])
    assert entities == [
        {"id": 20, "type": "project"},
        {"id": 19, "type": "project"},
        {"id": 18, "type": "project"},
    ]

    # Query the unique values for a set of fields in a column
    assert db.get_unique("user", ["age"]) == [{"age": 20}, {"age": 21}, {"age": 22}]


def test_invalid_schema(db: sqldb.SQLiteDatabase):
    db._initialise(
        """
    create table project (
        id          integer primary key autoincrement not null,
        name        text not null UNIQUE
    );
    """
    )

    # Invalid table
    with pytest.raises(sqldb.InvalidSchema):
        db.get_one("company")

    # Invalid field
    with pytest.raises(sqldb.InvalidSchema):
        db.get_one("project", fields=["address"])


def test_transactions(db: sqldb.SQLiteDatabase):
    db._initialise(
        """
        create table project (
            id          integer primary key autoincrement not null,
            name        text not null UNIQUE
        );
        """
    )

    db.create("project", {"name": "one"})
    assert db.get_one("project") == {"type": "project", "name": "one", "id": 1}
    assert not db._connection.in_transaction

    with db.transaction():
        db.create("project", {"name": "two"})
        # Should still be in a transaction, but able to read the current modified state
        assert db._connection.in_transaction
        assert db.get("project")[1] == [
            {"type": "project", "name": "one", "id": 1},
            {"type": "project", "name": "two", "id": 2},
        ]

        db.create("project", {"name": "three"})

    # Transaction should be completed and all data committed
    assert not db._connection.in_transaction
    assert db.get("project")[1] == [
        {"type": "project", "name": "one", "id": 1},
        {"type": "project", "name": "two", "id": 2},
        {"type": "project", "name": "three", "id": 3},
    ]

    try:
        with db.transaction():
            db.update("project", 2, {"name": "four"})
            assert db.get_one("project", filters=[{"eq": {"id": 2}}])["name"] == "four"
            # Violates uniqueness constraint, will raise an error
            db.create("project", {"name": "one"})
    except sqlite3.IntegrityError as e:
        assert str(e) == "UNIQUE constraint failed: project.name"

    # The update should have been rolled back and not committed
    assert db.get_one("project", filters=[{"eq": {"id": 2}}])["name"] == "two"


def test_joins(db: sqldb.SQLiteDatabase):
    db._initialise(
        """
        create table project (
            id          integer primary key autoincrement not null,
            name        text
        );
        create table department (
            id          integer primary key autoincrement not null,
            name        text
        );
        create table person (
            id          integer primary key autoincrement not null,
            name        text,
            department_id    integer
        );
        create table task (
            id          integer primary key autoincrement not null,
            name        text,
            project_id  integer,
            assignee    integer
        );
        """
    )

    # TODO: Multi-joins. For one to many this would return multiple rows that
    # should be combined. The concept of `get_one` may want to understand this
    # and return the list of joined rows.
    project_id = db.create("project", {"name": "MyProject"})
    department_id = db.create("department", {"name": "DepartmentA"})
    person_id = db.create("person", {"name": "mshaw", "department_id": department_id})
    task_id = db.create(
        "task", {"name": "fix it", "assignee": person_id, "project_id": project_id}
    )

    row = db.get_one(
        "task",
        joins=[
            {"table": "project"},
            {
                "table": "person",
                "dst_field": "assignee",
                "joins": [{"table": "department"}],
            },
        ],
    )
    assert row == {
        "type": "task",
        "id": task_id,
        "name": "fix it",
        "project": {"type": "project", "id": project_id, "name": "MyProject"},
        "person": {
            "type": "person",
            "id": person_id,
            "name": "mshaw",
            "department": {
                "type": "department",
                "id": department_id,
                "name": "DepartmentA",
            },
        },
    }
