"""
Database Backup & Restore Manager (#11)

Uses DuckDB's EXPORT/IMPORT DATABASE for reliable backup/restore.
"""

import logging
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

import duckdb

from ..config import config
from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)


class BackupManager:
    """Manages database backups as ZIP archives."""

    def __init__(self, db_manager=None):
        self.backup_dir = config.DATA_DIR / "backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = str(config.DB_PATH)
        if db_manager is None:
            db_manager = DatabaseManager()
        self.db_manager = db_manager

    def create_backup(self) -> dict:
        """Create a backup using DuckDB EXPORT DATABASE -> ZIP.

        Returns:
            Dict with filename, path, and size info.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_dir = self.backup_dir / f"_export_{timestamp}"
        zip_filename = f"expirytrack_backup_{timestamp}.zip"
        zip_path = self.backup_dir / zip_filename

        try:
            export_dir.mkdir(parents=True, exist_ok=True)
            safe_export = str(export_dir).replace("'", "''")

            with self.db_manager.get_connection() as conn:
                conn.execute(f"EXPORT DATABASE '{safe_export}' (FORMAT PARQUET)")

            # Create ZIP from exported files
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for root, _dirs, files in os.walk(export_dir):
                    for fname in files:
                        file_path = os.path.join(root, fname)
                        arcname = os.path.relpath(file_path, export_dir)
                        zf.write(file_path, arcname)

            size_mb = round(zip_path.stat().st_size / (1024 * 1024), 2)
            logger.info(f"Backup created: {zip_filename} ({size_mb} MB)")

            return {
                "filename": zip_filename,
                "path": str(zip_path),
                "size_mb": size_mb,
                "created_at": timestamp,
            }

        finally:
            # Clean up export directory
            if export_dir.exists():
                shutil.rmtree(export_dir, ignore_errors=True)

    def restore_backup(self, zip_path: str) -> bool:
        """Restore database from a ZIP backup.

        1. Creates a safety backup of current DB
        2. Extracts ZIP to temp dir
        3. Imports into a new DuckDB file
        4. Replaces current DB
        """
        zip_path = Path(zip_path)
        if not zip_path.exists():
            raise FileNotFoundError(f"Backup file not found: {zip_path}")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extract_dir = self.backup_dir / f"_restore_{timestamp}"
        db_path = Path(self.db_path)

        try:
            # Safety backup
            safety_path = self.backup_dir / f"pre_restore_safety_{timestamp}.duckdb"
            if db_path.exists():
                shutil.copy2(db_path, safety_path)
                logger.info(f"Safety backup: {safety_path}")

            # Extract ZIP
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)

            safe_extract = str(extract_dir).replace("'", "''")

            # Create new DB from import
            new_db = self.backup_dir / f"_restored_{timestamp}.duckdb"
            conn = duckdb.connect(str(new_db))
            try:
                conn.execute(f"IMPORT DATABASE '{safe_extract}'")
            finally:
                conn.close()

            # Replace current DB
            # Close any WAL first
            wal_path = Path(self.db_path + ".wal")
            if db_path.exists():
                db_path.unlink()
            if wal_path.exists():
                wal_path.unlink()
            shutil.move(str(new_db), str(db_path))

            logger.info(f"Database restored from {zip_path.name}")
            return True

        except Exception as e:
            logger.error(f"Restore failed: {e}")
            raise

        finally:
            if extract_dir.exists():
                shutil.rmtree(extract_dir, ignore_errors=True)

    def list_backups(self) -> list[dict]:
        """List all backup ZIP files."""
        backups = []
        for f in sorted(self.backup_dir.glob("expirytrack_backup_*.zip"), reverse=True):
            size_mb = round(f.stat().st_size / (1024 * 1024), 2)
            # Parse timestamp from filename
            try:
                ts_str = f.stem.replace("expirytrack_backup_", "")
                created = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").isoformat()
            except ValueError:
                created = None

            backups.append(
                {
                    "filename": f.name,
                    "size_mb": size_mb,
                    "created_at": created,
                }
            )
        return backups

    def delete_backup(self, filename: str) -> bool:
        """Delete a backup file."""
        # Sanitize filename to prevent path traversal
        safe_name = Path(filename).name
        path = self.backup_dir / safe_name
        if path.exists() and path.suffix == ".zip":
            path.unlink()
            logger.info(f"Deleted backup: {safe_name}")
            return True
        return False

    def get_backup_path(self, filename: str) -> Path | None:
        """Get full path to a backup file (with path traversal protection)."""
        safe_name = Path(filename).name
        path = self.backup_dir / safe_name
        if path.exists() and path.suffix == ".zip":
            # Verify it's within backup_dir
            real = path.resolve()
            if str(real).startswith(str(self.backup_dir.resolve())):
                return real
        return None
