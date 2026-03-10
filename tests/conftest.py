"""
tests/conftest.py
Session-wide test setup: initialises a temporary SQLite database,
applies the full schema, and seeds Elo ratings for top players so
that all modules that call database.db.get_elo() have a working
database during tests.
"""

import os
import tempfile
import threading
import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_test_db():
    """
    Create a temporary database, apply schema, and seed top-player Elo ratings.

    This fixture runs once for the entire test session and patches
    `database.db.DATABASE_PATH` in-place so every subsequent call to
    `get_conn()` (and therefore `get_elo()`, `upsert_elo()`, etc.) uses
    the temporary file rather than the production database.
    """
    import database.db as db_module
    import config as config_module

    # Create a fresh temporary database file
    fd, test_db_path = tempfile.mkstemp(suffix=".db", prefix="tennisedge_test_")
    os.close(fd)

    # Patch DATABASE_PATH in both places it's stored
    original_db_path = db_module.DATABASE_PATH
    db_module.DATABASE_PATH = test_db_path
    config_module.DATABASE_PATH = test_db_path

    # Discard any cached thread-local connection so the next call to
    # get_conn() opens a fresh connection to the test database
    db_module._local = threading.local()

    # Initialise schema (creates all tables including player_elo)
    from database.db import init_schema
    init_schema()

    # Seed Elo ratings for top players so predict() returns realistic values
    from models.elo_model import seed_top_players
    seed_top_players()

    yield test_db_path

    # Restore original path and clean up
    db_module.DATABASE_PATH = original_db_path
    config_module.DATABASE_PATH = original_db_path
    db_module._local = threading.local()

    try:
        os.unlink(test_db_path)
    except OSError:
        pass
