import os

import pytest

import sqldb


@pytest.fixture(scope="function")
def db(tmpdir):
    return sqldb.SQLiteDatabase(os.path.join(tmpdir, "sqldb.sqlite"))


def test_crud(db):
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


def test_get_methods(db):
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


def test_invalid_schema(db):
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
