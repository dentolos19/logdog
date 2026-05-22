"""Tests for log group and file deletion with cascading cleanup.

Verifies that:
1. Model cascade/ondelete settings are correctly defined
2. The delete_log_group handler drops megabase tables and enqueues orphan asset cleanup
3. The delete_log_file_route handler cleans up megabase tables for the file
4. Edge cases don't crash
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from lib.models import (
    LogFile,
    LogGroup,
    LogMessage,
    LogProcess,
    LogReport,
    LogTable,
)


# ── Helpers ─────────────────────────────────────────────────────


def assert_relationship_has_cascade(model, attr_name: str, cascade_flag: str) -> None:
    """Assert that a SQLAlchemy relationship includes a specific cascade flag."""
    rels = model.__mapper__.relationships
    for rel in rels:
        if rel.key == attr_name:
            assert cascade_flag in str(rel.cascade), (
                f"{model.__name__}.{attr_name} cascade does not contain '{cascade_flag}'. Got: '{rel.cascade}'"
            )
            return
    pytest.fail(f"No relationship '{attr_name}' found on {model.__name__}")


def assert_fk_ondelete(model, column_name: str, expected: str) -> None:
    """Assert that a FK column has a specific ondelete setting."""
    col = model.__table__.columns.get(column_name)
    assert col is not None, f"Column {model.__name__}.{column_name} not found"
    for fk in col.foreign_keys:
        assert fk.ondelete == expected, (
            f"{model.__name__}.{column_name} ondelete expected '{expected}', got '{fk.ondelete}'"
        )
        return
    pytest.fail(f"No foreign key on {model.__name__}.{column_name}")


# ── Database mock factory ───────────────────────────────────────


def make_db_mock() -> MagicMock:
    """Create a MagicMock that looks like a SQLAlchemy Session."""
    db = MagicMock()
    # We'll return query results via explicit setup in each test
    return db


# ── Static model verification ────────────────────────────────────


class TestModelCascadeSettings:
    """Verify cascade and ondelete settings on all models."""

    def test_log_group_cascades_files(self):
        assert_relationship_has_cascade(LogGroup, "files", "delete-orphan")

    def test_log_group_cascades_tables(self):
        assert_relationship_has_cascade(LogGroup, "tables", "delete-orphan")

    def test_log_group_cascades_messages(self):
        assert_relationship_has_cascade(LogGroup, "messages", "delete-orphan")

    def test_log_group_cascades_processes(self):
        assert_relationship_has_cascade(LogGroup, "processes", "delete-orphan")

    def test_log_group_cascades_reports(self):
        assert_relationship_has_cascade(LogGroup, "reports", "delete-orphan")

    def test_log_file_cascades_processes(self):
        assert_relationship_has_cascade(LogFile, "processes", "delete-orphan")

    def test_log_file_fk_ondelete_cascade(self):
        assert_fk_ondelete(LogFile, "group_id", "CASCADE")

    def test_log_table_fk_ondelete_cascade(self):
        assert_fk_ondelete(LogTable, "group_id", "CASCADE")

    def test_log_message_fk_ondelete_cascade(self):
        assert_fk_ondelete(LogMessage, "group_id", "CASCADE")

    def test_log_process_fk_ondelete_cascade(self):
        assert_fk_ondelete(LogProcess, "group_id", "CASCADE")

    def test_log_process_fk_ondelete_set_null(self):
        assert_fk_ondelete(LogProcess, "file_id", "SET NULL")

    def test_log_report_fk_ondelete_cascade(self):
        assert_fk_ondelete(LogReport, "group_id", "CASCADE")


# ── Handler-level tests (mocked external dependencies) ──────────


@pytest.fixture
def mock_megabase():
    """Mock all megabase-related dependencies."""
    with (
        patch("routes.logs.MegabaseSessionLocal") as mock_session_factory,
        patch("routes.logs.init_megabase") as mock_init,
        patch("routes.logs.megabase_drop_table") as mock_drop,
    ):
        mock_session = MagicMock()
        mock_session_factory.return_value = mock_session
        yield {
            "session_factory": mock_session_factory,
            "init": mock_init,
            "drop": mock_drop,
            "session": mock_session,
        }


class TestDeleteLogGroupHandler:
    """Tests for the delete_log_group endpoint handler."""

    def test_drops_megabase_tables(self, mock_megabase):
        """Should drop every megabase table referenced by LogTable rows."""
        from fastapi import BackgroundTasks
        from routes.logs import delete_log_group

        mock_group = MagicMock()
        mock_group.id = uuid.uuid4()
        mock_db = MagicMock()

        table_id_1 = str(uuid.uuid4())
        table_id_2 = str(uuid.uuid4())

        mock_table_1 = MagicMock()
        mock_table_1.table = table_id_1
        mock_table_2 = MagicMock()
        mock_table_2.table = table_id_2

        def mock_query(model):
            if model is LogTable:
                q = MagicMock()
                q.filter.return_value.all.return_value = [mock_table_1, mock_table_2]
                return q
            if model is LogFile:
                q = MagicMock()
                q.filter.return_value.all.return_value = []
                return q
            return MagicMock()

        mock_db.query = mock_query
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()

        with patch("routes.logs._require_owned_group", return_value=mock_group):
            delete_log_group(
                group_id=str(mock_group.id),
                background_tasks=BackgroundTasks(),
                current_user=mock_user,
                database=mock_db,
            )

        mock_megabase["drop"].assert_any_call(mock_megabase["session"], table_id_1)
        mock_megabase["drop"].assert_any_call(mock_megabase["session"], table_id_2)
        assert mock_megabase["drop"].call_count == 2
        mock_db.delete.assert_called_once_with(mock_group)
        mock_db.commit.assert_called_once()

    def test_enqueues_orphan_asset_cleanup(self, mock_megabase):
        """Should pass orphaned asset IDs to _delete_orphan_assets via background task."""
        from fastapi import BackgroundTasks
        from routes.logs import _delete_orphan_assets, delete_log_group

        mock_group = MagicMock()
        mock_group.id = uuid.uuid4()
        mock_db = MagicMock()

        asset_id_1 = uuid.uuid4()
        asset_id_2 = uuid.uuid4()

        mock_file_1 = MagicMock()
        mock_file_1.asset_id = asset_id_1
        mock_file_2 = MagicMock()
        mock_file_2.asset_id = asset_id_2

        def mock_query(model):
            if model is LogTable:
                q = MagicMock()
                q.filter.return_value.all.return_value = []
                return q
            if model is LogFile:
                q = MagicMock()
                q.filter.return_value.all.return_value = [mock_file_1, mock_file_2]
                return q
            return MagicMock()

        mock_db.query = mock_query
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()

        bt = BackgroundTasks()

        with patch("routes.logs._require_owned_group", return_value=mock_group):
            delete_log_group(
                group_id=str(mock_group.id),
                background_tasks=bt,
                current_user=mock_user,
                database=mock_db,
            )

        # BackgroundTasks.add_task stores BackgroundTask objects
        assert len(bt.tasks) == 1
        task = bt.tasks[0]
        assert task.func is _delete_orphan_assets
        assert set(task.args[0]) == {asset_id_1, asset_id_2}

    def test_skips_megabase_when_no_tables(self, mock_megabase):
        """Should not call megabase functions when the group has no LogTable rows."""
        from fastapi import BackgroundTasks
        from routes.logs import delete_log_group

        mock_group = MagicMock()
        mock_group.id = uuid.uuid4()
        mock_db = MagicMock()

        def mock_query(model):
            q = MagicMock()
            q.filter.return_value.all.return_value = []
            return q

        mock_db.query = mock_query
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()

        with patch("routes.logs._require_owned_group", return_value=mock_group):
            delete_log_group(
                group_id=str(mock_group.id),
                background_tasks=BackgroundTasks(),
                current_user=mock_user,
                database=mock_db,
            )

        mock_megabase["init"].assert_not_called()
        mock_megabase["drop"].assert_not_called()
        mock_db.delete.assert_called_once_with(mock_group)
        mock_db.commit.assert_called_once()

    def test_deletes_group_and_commits(self, mock_megabase):
        """Should call database.delete(group) and commit."""
        from fastapi import BackgroundTasks
        from routes.logs import delete_log_group

        mock_group = MagicMock()
        mock_group.id = uuid.uuid4()
        mock_db = MagicMock()

        def mock_query(model):
            q = MagicMock()
            q.filter.return_value.all.return_value = []
            return q

        mock_db.query = mock_query
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()

        with patch("routes.logs._require_owned_group", return_value=mock_group):
            delete_log_group(
                group_id=str(mock_group.id),
                background_tasks=BackgroundTasks(),
                current_user=mock_user,
                database=mock_db,
            )

        mock_db.delete.assert_called_once_with(mock_group)
        mock_db.commit.assert_called_once()


class TestDeleteLogFileRouteHandler:
    """Tests for the delete_log_file_route endpoint handler."""

    def test_cleans_up_megabase_tables_before_deletion(self):
        """Should call _cleanup_generated_tables_for_file before deleting the file."""
        from routes.logs import delete_log_file_route

        mock_group = MagicMock()
        mock_group.id = uuid.uuid4()
        mock_log_file = MagicMock()
        mock_log_file.id = uuid.uuid4()
        mock_log_file.asset_id = uuid.uuid4()
        mock_db = MagicMock()

        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()

        with (
            patch("routes.logs._require_owned_group", return_value=mock_group),
            patch("routes.logs._require_owned_file", return_value=(mock_log_file, MagicMock())),
            patch("routes.logs._cleanup_generated_tables_for_file") as mock_cleanup,
        ):
            delete_log_file_route(
                group_id=str(mock_group.id),
                file_id=str(mock_log_file.id),
                current_user=mock_user,
                database=mock_db,
            )

        mock_cleanup.assert_called_once_with(
            database=mock_db,
            group_id=str(mock_group.id),
            file_id=str(mock_log_file.id),
        )

    def test_deletes_log_file_and_asset(self):
        """Should delete the LogFile, commit, and delete the orphan asset."""
        from routes.logs import delete_log_file_route

        mock_group = MagicMock()
        mock_group.id = uuid.uuid4()
        mock_log_file = MagicMock()
        mock_log_file.id = uuid.uuid4()
        mock_log_file.asset_id = uuid.uuid4()
        mock_asset = MagicMock()

        mock_db = MagicMock()
        # Set up the remaining-links query to return 0 (no other files share this asset)
        mock_db.query.return_value.filter.return_value.count.return_value = 0

        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()

        with (
            patch("routes.logs._require_owned_group", return_value=mock_group),
            patch("routes.logs._require_owned_file", return_value=(mock_log_file, mock_asset)),
            patch("routes.logs._cleanup_generated_tables_for_file"),
            patch("routes.logs.delete_file") as mock_delete_file,
        ):
            delete_log_file_route(
                group_id=str(mock_group.id),
                file_id=str(mock_log_file.id),
                current_user=mock_user,
                database=mock_db,
            )

        mock_db.delete.assert_called_with(mock_log_file)
        mock_db.commit.assert_called_once()
        # With 0 remaining links, the asset should be deleted
        mock_delete_file.assert_called_once_with(asset_id=mock_log_file.asset_id, db=mock_db)

    def test_does_not_delete_shared_asset(self):
        """Should skip asset deletion when other LogFile rows reference the same asset."""
        from routes.logs import delete_log_file_route

        mock_group = MagicMock()
        mock_group.id = uuid.uuid4()
        mock_log_file = MagicMock()
        mock_log_file.id = uuid.uuid4()
        mock_log_file.asset_id = uuid.uuid4()
        mock_asset = MagicMock()

        mock_db = MagicMock()
        # Return 1 remaining link so delete_file is NOT called
        mock_db.query.return_value.filter.return_value.count.return_value = 1

        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()

        with (
            patch("routes.logs._require_owned_group", return_value=mock_group),
            patch("routes.logs._require_owned_file", return_value=(mock_log_file, mock_asset)),
            patch("routes.logs._cleanup_generated_tables_for_file"),
            patch("routes.logs.delete_file") as mock_delete_file,
        ):
            delete_log_file_route(
                group_id=str(mock_group.id),
                file_id=str(mock_log_file.id),
                current_user=mock_user,
                database=mock_db,
            )

        mock_db.delete.assert_called_with(mock_log_file)
        mock_db.commit.assert_called_once()
        mock_delete_file.assert_not_called()

    def test_handles_no_completed_processes_gracefully(self):
        """Should not raise when the file has no completed processes."""
        from routes.logs import delete_log_file_route

        mock_group = MagicMock()
        mock_group.id = uuid.uuid4()
        mock_log_file = MagicMock()
        mock_log_file.id = uuid.uuid4()
        mock_log_file.asset_id = uuid.uuid4()
        mock_db = MagicMock()

        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()

        with (
            patch("routes.logs._require_owned_group", return_value=mock_group),
            patch("routes.logs._require_owned_file", return_value=(mock_log_file, MagicMock())),
            patch("routes.logs._cleanup_generated_tables_for_file") as mock_cleanup,
        ):
            # Should not raise
            delete_log_file_route(
                group_id=str(mock_group.id),
                file_id=str(mock_log_file.id),
                current_user=mock_user,
                database=mock_db,
            )

        mock_cleanup.assert_called_once()
