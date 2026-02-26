"""Tests for BackupManager — create, list, delete, get_backup_path."""

from unittest.mock import patch

from src.backup.manager import BackupManager


def _seed_data(db):
    """Insert minimal data so EXPORT DATABASE has something to back up."""
    with db.get_connection() as conn:
        conn.execute("""
            INSERT INTO instruments (instrument_key, symbol, name, exchange)
            VALUES ('NSE_INDEX|Nifty 50', 'NIFTY', 'Nifty 50', 'NSE')
        """)


def test_create_backup(tmp_db, tmp_path):
    _seed_data(tmp_db)
    with patch.object(BackupManager, "__init__", lambda self, *a, **kw: None):
        mgr = BackupManager.__new__(BackupManager)
        mgr.backup_dir = tmp_path / "backups"
        mgr.backup_dir.mkdir()
        mgr.db_path = str(tmp_db.db_path)
        mgr.db_manager = tmp_db

    result = mgr.create_backup()
    assert "filename" in result
    assert result["filename"].startswith("expirytrack_backup_")
    assert result["filename"].endswith(".zip")
    assert result["size_mb"] >= 0
    assert (mgr.backup_dir / result["filename"]).exists()


def test_list_backups(tmp_db, tmp_path):
    _seed_data(tmp_db)
    with patch.object(BackupManager, "__init__", lambda self, *a, **kw: None):
        mgr = BackupManager.__new__(BackupManager)
        mgr.backup_dir = tmp_path / "backups"
        mgr.backup_dir.mkdir()
        mgr.db_path = str(tmp_db.db_path)
        mgr.db_manager = tmp_db

    # Initially empty
    assert mgr.list_backups() == []

    # Create one
    mgr.create_backup()
    backups = mgr.list_backups()
    assert len(backups) == 1
    assert "filename" in backups[0]
    assert "size_mb" in backups[0]
    assert "created_at" in backups[0]


def test_delete_backup(tmp_db, tmp_path):
    _seed_data(tmp_db)
    with patch.object(BackupManager, "__init__", lambda self, *a, **kw: None):
        mgr = BackupManager.__new__(BackupManager)
        mgr.backup_dir = tmp_path / "backups"
        mgr.backup_dir.mkdir()
        mgr.db_path = str(tmp_db.db_path)
        mgr.db_manager = tmp_db

    result = mgr.create_backup()
    filename = result["filename"]

    # Delete it
    assert mgr.delete_backup(filename) is True
    assert mgr.list_backups() == []

    # Delete non-existent
    assert mgr.delete_backup("nonexistent.zip") is False


def test_get_backup_path(tmp_db, tmp_path):
    _seed_data(tmp_db)
    with patch.object(BackupManager, "__init__", lambda self, *a, **kw: None):
        mgr = BackupManager.__new__(BackupManager)
        mgr.backup_dir = tmp_path / "backups"
        mgr.backup_dir.mkdir()
        mgr.db_path = str(tmp_db.db_path)
        mgr.db_manager = tmp_db

    result = mgr.create_backup()
    filename = result["filename"]

    # Valid file
    path = mgr.get_backup_path(filename)
    assert path is not None
    assert path.exists()

    # Non-existent
    assert mgr.get_backup_path("nonexistent.zip") is None

    # Path traversal attempt
    assert mgr.get_backup_path("../../../etc/passwd") is None


def test_path_traversal_prevention(tmp_db, tmp_path):
    """Delete and get_backup_path should reject path traversal."""
    with patch.object(BackupManager, "__init__", lambda self, *a, **kw: None):
        mgr = BackupManager.__new__(BackupManager)
        mgr.backup_dir = tmp_path / "backups"
        mgr.backup_dir.mkdir()
        mgr.db_path = str(tmp_db.db_path)
        mgr.db_manager = tmp_db

    assert mgr.delete_backup("../../etc/passwd") is False
    assert mgr.get_backup_path("../../etc/passwd") is None
