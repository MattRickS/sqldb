import os

import sqldb


class MyDB(sqldb.SQLiteDatabase):
    SQL_SCHEMA = ""

    def _initialise(self):
        with self._connection:
            self._connection.executescript(self.SQL_SCHEMA)


def test_crud():
    MyDB.SQL_SCHEMA = """
    create table project (
        id          integer primary key autoincrement not null,
        display     text not null,
        name        text not null UNIQUE,
        description text,
        created     timestamp default current_timestamp,
        creator     text not null
    );
    """
    db = MyDB("pytest.db")

    try:
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
        assert project == {"id": 1, "display": "My Project", "description": None}

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
        assert projects == [{"id": 1, "name": "MyProject", "creator": "mshaw"}]

        # Deleting
        result = db.delete("project", uid)
        assert result is True
        assert db.get_one("project") == {}
    finally:
        os.remove("pytest.db")
