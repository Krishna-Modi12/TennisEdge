import unittest
from unittest.mock import patch

import database.db as db


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows):
        self._rows = rows
        self.rollback_called = False

    def execute(self, _sql, _params=None):
        return _Cursor(self._rows)

    def rollback(self):
        self.rollback_called = True


class DatabaseHelpersTests(unittest.TestCase):
    def test_get_all_user_telegram_ids_returns_sorted_rows(self):
        conn = _Conn(rows=[(111,), (222,), (333,)])
        with patch("database.db.get_conn", return_value=conn):
            ids = db.get_all_user_telegram_ids()
        self.assertEqual(ids, [111, 222, 333])


if __name__ == "__main__":
    unittest.main()
