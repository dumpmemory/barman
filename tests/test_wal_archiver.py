# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2025
#
# This file is part of Barman.
#
# Barman is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Barman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.
import os

import pytest
from mock import ANY, MagicMock, call, patch
from testing_helpers import build_backup_manager, build_test_backup_info, caplog_reset

import barman.xlog
from barman.exceptions import (
    AbortedRetryHookScript,
    ArchiverFailure,
    CommandFailedException,
    DuplicateWalFile,
    MatchingDuplicateWalFile,
)
from barman.infofile import WalFileInfo
from barman.process import ProcessInfo
from barman.server import CheckOutputStrategy
from barman.wal_archiver import (
    FileWalArchiver,
    LocalWalStorageStrategy,
    StreamingWalArchiver,
    WalArchiverQueue,
    WalStorageStrategy,
)


# noinspection PyMethodMayBeStatic
class TestFileWalArchiver(object):
    def test_init(self):
        """
        Basic init test for the FileWalArchiver class
        """
        backup_manager = build_backup_manager()
        FileWalArchiver(backup_manager)

    def test_get_remote_status(self):
        """
        Basic test for the check method of the FileWalArchiver class
        """
        # Create a backup_manager
        backup_manager = build_backup_manager()
        # Set up mock responses
        postgres = backup_manager.server.postgres
        postgres.get_setting.side_effect = ["value1", "value2"]
        postgres.get_archiver_stats.return_value = {"pg_stat_archiver": "value3"}
        # Instantiate a FileWalArchiver obj
        archiver = FileWalArchiver(backup_manager)
        result = {
            "archive_mode": "value1",
            "archive_command": "value2",
            "pg_stat_archiver": "value3",
        }
        # Compare results of the check method
        assert archiver.get_remote_status() == result

    @patch("barman.wal_archiver.FileWalArchiver.get_remote_status")
    def test_check(self, remote_mock, capsys):
        """
        Test management of check_postgres view output

        :param remote_mock: mock get_remote_status function
        :param capsys: retrieve output from console
        """
        # Create a backup_manager
        backup_manager = build_backup_manager()
        # Set up mock responses
        postgres = backup_manager.server.postgres
        postgres.server_version = 90501
        # Instantiate a FileWalArchiver obj
        archiver = FileWalArchiver(backup_manager)
        # Prepare the output check strategy
        strategy = CheckOutputStrategy()
        # Case: no reply by PostgreSQL
        remote_mock.return_value = {
            "archive_mode": None,
            "archive_command": None,
        }
        # Expect no output from check
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert out == ""
        # Case: correct configuration
        remote_mock.return_value = {
            "archive_mode": "on",
            "archive_command": "wal to archive",
            "is_archiving": True,
            "incoming_wals_count": 0,
        }
        # Expect out: all parameters: OK
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tarchive_mode: OK\n"
            "\tarchive_command: OK\n"
            "\tcontinuous archiving: OK\n"
        )

        # Case: archive_command value is not acceptable
        remote_mock.return_value = {
            "archive_command": None,
            "archive_mode": "on",
            "is_archiving": False,
            "incoming_wals_count": 0,
        }
        # Expect out: some parameters: FAILED
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tarchive_mode: OK\n"
            "\tarchive_command: FAILED "
            "(please set it accordingly to documentation)\n"
        )
        # Case: all but is_archiving ok
        remote_mock.return_value = {
            "archive_mode": "on",
            "archive_command": "wal to archive",
            "is_archiving": False,
            "incoming_wals_count": 0,
        }
        # Expect out: all parameters: OK
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tarchive_mode: OK\n"
            "\tarchive_command: OK\n"
            "\tcontinuous archiving: FAILED\n"
        )
        # Case: too many wal files in the incoming queue
        archiver.config.max_incoming_wals_queue = 10
        remote_mock.return_value = {
            "archive_mode": "on",
            "archive_command": "wal to archive",
            "is_archiving": False,
            "incoming_wals_count": 20,
        }
        # Expect out: the wals incoming queue is too big
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tarchive_mode: OK\n"
            "\tarchive_command: OK\n"
            "\tcontinuous archiving: FAILED\n"
        )

    @patch("os.unlink")
    @patch("barman.wal_archiver.FileWalArchiver.get_next_batch")
    @patch("datetime.datetime")
    def test_archive(
        self,
        datetime_mock,
        get_next_batch_mock,
        unlink_mock,
        capsys,
        caplog,
    ):
        """
        Test FileWalArchiver.archive method
        """
        # See all logs
        caplog.set_level(0)

        fxlogdb_mock = MagicMock()
        backup_manager = MagicMock()
        archiver = FileWalArchiver(backup_manager)
        archiver.config.name = "test_server"
        archiver.config.errors_directory = "/server/errors"

        wal_info = WalFileInfo(name="test_wal_file")
        wal_info.orig_filename = "test_wal_file"

        batch = WalArchiverQueue([wal_info], total_size=1)
        assert batch.total_size == 1
        assert batch.run_size == 1
        get_next_batch_mock.return_value = batch
        archiver.storage_strategy = MagicMock(
            save=MagicMock(side_effect=DuplicateWalFile)
        )
        datetime_mock.utcnow.return_value.strftime.return_value = "test_time"

        archiver.archive(fxlogdb_mock)

        out, err = capsys.readouterr()
        assert (
            "\tError: %s is already present in server %s. "
            "File moved to errors directory." % (wal_info.name, archiver.config.name)
        ) in out

        assert (
            "\tError: %s is already present in server %s. "
            "File moved to errors directory." % (wal_info.name, archiver.config.name)
        ) in caplog.text

        archiver.storage_strategy = MagicMock(
            save=MagicMock(side_effect=MatchingDuplicateWalFile)
        )
        archiver.archive(fxlogdb_mock)
        unlink_mock.assert_called_with(wal_info.orig_filename)

        # Test batch errors
        caplog_reset(caplog)
        batch.errors = ["testfile_1", "testfile_2"]
        archiver.storage_strategy = MagicMock(
            save=MagicMock(side_effect=DuplicateWalFile)
        )
        archiver.archive(fxlogdb_mock)
        out, err = capsys.readouterr()

        assert (
            "Some unknown objects have been found while "
            "processing xlog segments for %s. "
            "Objects moved to errors directory:" % archiver.config.name
        ) in out

        assert (
            "Archiver is about to move %s unexpected file(s) to errors "
            "directory for %s from %s"
            % (len(batch.errors), archiver.config.name, archiver.name)
        ) in caplog.text

        assert (
            "Moving unexpected file for %s from %s: %s"
            % (archiver.config.name, archiver.name, "testfile_1")
        ) in caplog.text

        assert (
            "Moving unexpected file for %s from %s: %s"
            % (archiver.config.name, archiver.name, "testfile_2")
        ) in caplog.text

        archiver.server.move_wal_file_to_errors_directory.assert_any_call(
            "testfile_1", "testfile_1", "unknown"
        )

        archiver.server.move_wal_file_to_errors_directory.assert_any_call(
            "testfile_2", "testfile_2", "unknown"
        )

    @patch("os.fsync")
    @patch("barman.wal_archiver.FileWalArchiver.get_next_batch")
    def test_archive_batch(
        self, get_next_batch_mock, fsync_mock, caplog
    ):
        """
        Test archive using batch limit
        """
        # See all logs
        caplog.set_level(0)

        # Setup the test
        fxlogdb_mock = MagicMock()
        backup_manager = MagicMock()
        archiver = FileWalArchiver(backup_manager)
        archiver.storage_strategy = MagicMock()
        archiver.config.name = "test_server"

        wal_info = WalFileInfo(name="test_wal_file")
        wal_info.orig_filename = "test_wal_file"
        wal_info2 = WalFileInfo(name="test_wal_file2")
        wal_info2.orig_filename = "test_wal_file2"

        # Test queue with batch of 2 and 4 in total
        batch = WalArchiverQueue([wal_info, wal_info2], total_size=4)
        assert batch.total_size == 4
        assert batch.run_size == 2

        get_next_batch_mock.return_value = batch
        archiver.archive(fxlogdb_mock)
        # check the log for messages
        assert (
            "Found %s xlog segments from %s for %s."
            " Archive a batch of %s segments in this run."
            % (batch.total_size, archiver.name, archiver.config.name, batch.run_size)
        ) in caplog.text
        assert (
            "Batch size reached (%s) - "
            "Exit %s process for %s"
            % (batch.batch_size, archiver.name, archiver.config.name)
        ) in caplog.text

    # TODO: The following test should be splitted in two
    # the BackupManager part and the FileWalArchiver part
    def test_base_archive_wal(self, tmpdir):
        """
        Basic archiving test

        Provide a WAL file and check for the correct location of the file at
        the end of the process
        """
        # Build a real backup manager
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
            begin_wal="000000010000000000000001",
        )
        b_info.save()
        backup_manager.server.get_backup.return_value = b_info
        backup_manager.compression_manager.get_default_compressor.return_value = None
        backup_manager.compression_manager.get_compressor.return_value = None
        # Build the basic folder structure and files
        basedir = tmpdir.join("main")
        incoming_dir = basedir.join("incoming")
        archive_dir = basedir.join("wals")
        xlog_db = archive_dir.join("xlog.db")
        wal_name = "000000010000000000000001"
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        xlog_db_fileobj = xlog_db.open(mode="a")
        backup_manager.server.xlogdb.return_value.__enter__.return_value = xlog_db_fileobj
        backup_manager.server.use_wal_cloud_storage = False
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()
        wal_path = os.path.join(
            archive_dir.strpath, barman.xlog.hash_dir(wal_name), wal_name
        )
        # Check for the presence of the wal file in the wal catalog
        xlog_db_fileobj.flush()
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        # Check that the wal file have been moved from the incoming dir
        assert not os.path.exists(wal_file.strpath)
        # Check that the wal file have been archived to the expected location
        assert os.path.exists(wal_path)

    @pytest.fixture
    def mock_compression_registry(self):
        """
        Return a mock compression registry which omits the native gzip commands.
        This allows test_archive_wal to use a real compression manager without
        introducing a dependency on compression programs available in the shell.
        """
        registry = barman.compression.compression_registry.copy()
        registry.pop("gzip")
        registry.pop("pigz")
        return registry

    # TODO: The following test should be splitted in two
    # the BackupManager part and the FileWalArchiver part
    def test_archive_wal_no_backup(self, tmpdir, capsys):
        """
        Test archive-wal behaviour when there are no backups.

        Expect it to archive the files anyway
        """
        # Build a real backup manager
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        backup_manager.compression_manager.get_default_compressor.return_value = None
        backup_manager.compression_manager.get_compressor.return_value = None
        backup_manager.server.get_backup.return_value = None
        # Build the basic folder structure and files
        basedir = tmpdir.join("main")
        incoming_dir = basedir.join("incoming")
        archive_dir = basedir.join("wals")
        xlog_db = archive_dir.join("xlog.db")
        wal_name = "000000010000000000000001"
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        xlog_db_fileobj = xlog_db.open(mode="a")
        backup_manager.server.xlogdb.return_value.__enter__.return_value = xlog_db_fileobj
        backup_manager.server.use_wal_cloud_storage = False
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()

        # Check that the WAL file is present inside the wal catalog
        xlog_db_fileobj.flush()
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        wal_path = os.path.join(
            archive_dir.strpath, barman.xlog.hash_dir(wal_name), wal_name
        )
        # Check that the wal file have been archived
        assert os.path.exists(wal_path)
        out, err = capsys.readouterr()
        # Check the output for the archival of the wal file
        assert ("\t%s\n" % wal_name) in out

    # TODO: The following test should be splitted in two
    # the BackupManager part and the FileWalArchiver part
    def test_archive_wal_older_than_backup(self, tmpdir, capsys):
        """
        Test archive-wal command behaviour when the WAL files are older than
        the first backup of a server.

        Expect it to archive the files anyway
        """
        # Build a real backup manager and a fake backup
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
            begin_wal="000000010000000000000002",
        )
        b_info.save()
        # Build the basic folder structure and files
        backup_manager.compression_manager.get_default_compressor.return_value = None
        backup_manager.compression_manager.get_compressor.return_value = None
        backup_manager.server.get_backup.return_value = b_info
        basedir = tmpdir.join("main")
        incoming_dir = basedir.join("incoming")
        basedir.mkdir("errors")
        archive_dir = basedir.join("wals")
        xlog_db = archive_dir.join("xlog.db")
        wal_name = "000000010000000000000001"
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        xlog_db_fileobj = xlog_db.open(mode="a")
        backup_manager.server.xlogdb.return_value.__enter__.return_value = xlog_db_fileobj
        backup_manager.server.use_wal_cloud_storage = False
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()

        # Check that the WAL file is not present inside the wal catalog
        xlog_db_fileobj.flush()
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        wal_path = os.path.join(
            archive_dir.strpath, barman.xlog.hash_dir(wal_name), wal_name
        )
        # Check that the wal file have been archived
        assert os.path.exists(wal_path)
        # Check the output for the archival of the wal file
        out, err = capsys.readouterr()
        assert ("\t%s\n" % wal_name) in out

    # TODO: The following test should be splitted in two
    # the BackupManager part and the FileWalArchiver part
    def test_archive_wal_timeline_lower_than_backup(self, tmpdir, capsys):
        """
        Test archive-wal command behaviour when the WAL files are older than
        the first backup of a server.

        Expect it to archive the files anyway
        """
        # Build a real backup manager and a fake backup
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
            begin_wal="000000020000000000000002",
            timeline=2,
        )
        b_info.save()
        # Build the basic folder structure and files
        backup_manager.compression_manager.get_default_compressor.return_value = None
        backup_manager.compression_manager.get_compressor.return_value = None
        backup_manager.server.get_backup.return_value = b_info
        basedir = tmpdir.join("main")
        incoming_dir = basedir.join("incoming")
        basedir.mkdir("errors")
        archive_dir = basedir.join("wals")
        xlog_db = archive_dir.join("xlog.db")
        wal_name = "000000010000000000000001"
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        xlog_db_fileobj = xlog_db.open(mode="a")
        backup_manager.server.xlogdb.return_value.__enter__.return_value = xlog_db_fileobj
        backup_manager.server.use_wal_cloud_storage = False
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()

        # Check that the WAL file is present inside the wal catalog
        xlog_db_fileobj.flush()
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        wal_path = os.path.join(
            archive_dir.strpath, barman.xlog.hash_dir(wal_name), wal_name
        )
        # Check that the wal file have been archived
        assert os.path.exists(wal_path)
        # Check the output for the archival of the wal file
        out, err = capsys.readouterr()
        assert ("\t%s\n" % wal_name) in out

    @patch("barman.wal_archiver.glob")
    @patch("os.path.isfile")
    @patch("barman.wal_archiver.WalFileInfo.from_file")
    def test_get_next_batch(self, from_file_mock, isfile_mock, glob_mock):
        """
        Test the FileWalArchiver.get_next_batch method
        """

        # WAL batch no errors
        glob_mock.return_value = ["000000010000000000000001"]
        isfile_mock.return_value = True
        # This is an hack, instead of a WalFileInfo we use a simple string to
        # ease all the comparisons. The resulting string is the name enclosed
        # in colons. e.g. ":000000010000000000000001:"
        from_file_mock.side_effect = (
            lambda filename, compression_manager, unidentified_compression, encryption_manager, *args, **kwargs: ":%s:"
            % filename
        )

        backup_manager = build_backup_manager(name="TestServer")
        archiver = FileWalArchiver(backup_manager)
        backup_manager.server.archivers = [archiver]

        batch = archiver.get_next_batch()
        assert [":000000010000000000000001:"] == batch

        # WAL batch with errors
        wrong_file_name = "test_wrong_wal_file.2"
        glob_mock.return_value = ["test_wrong_wal_file.2"]
        batch = archiver.get_next_batch()
        assert [wrong_file_name] == batch.errors


# noinspection PyMethodMayBeStatic
class TestStreamingWalArchiver(object):
    def test_init(self):
        """
        Basic init test for the StreamingWalArchiver class
        """
        backup_manager = build_backup_manager()
        StreamingWalArchiver(backup_manager)

    @patch("barman.command_wrappers.PostgreSQLClient.find_command")
    def test_check_receivexlog_installed(self, find_command):
        """
        Test for the check method of the StreamingWalArchiver class
        """
        backup_manager = build_backup_manager()
        find_command.side_effect = CommandFailedException

        archiver = StreamingWalArchiver(backup_manager)
        result = archiver.get_remote_status()

        assert result == {
            "pg_receivexlog_installed": False,
            "pg_receivexlog_path": None,
            "pg_receivexlog_compatible": None,
            "pg_receivexlog_synchronous": None,
            "pg_receivexlog_version": None,
            "pg_receivexlog_supports_slots": None,
        }

        backup_manager.server.streaming.server_major_version = "9.2"
        find_command.side_effect = None
        find_command.return_value.cmd = "/some/path/to/pg_receivexlog"
        find_command.return_value.out = ""
        archiver.reset_remote_status()
        result = archiver.get_remote_status()

        assert result == {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_path": "/some/path/to/pg_receivexlog",
            "pg_receivexlog_compatible": None,
            "pg_receivexlog_synchronous": None,
            "pg_receivexlog_version": None,
            "pg_receivexlog_supports_slots": None,
        }

    @patch("barman.utils.which")
    @patch("barman.command_wrappers.Command")
    def test_check_receivexlog_is_compatible(self, command_mock, which_mock):
        """
        Test for the compatibility checks between versions of pg_receivexlog
        and PostgreSQL
        """
        # pg_receivexlog 9.2 is compatible only with PostgreSQL 9.2
        backup_manager = build_backup_manager()
        backup_manager.server.streaming.server_major_version = "9.2"
        archiver = StreamingWalArchiver(backup_manager)
        which_mock.return_value = "/some/path/to/pg_receivexlog"

        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.2.1"
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is True

        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.5.3"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is False

        # Every pg_receivexlog is compatible with older PostgreSQL
        backup_manager.server.streaming.server_major_version = "9.3"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.5.3"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is True

        backup_manager.server.streaming.server_major_version = "9.5"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.3.0"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is False

        # Check for minor versions
        backup_manager.server.streaming.server_major_version = "9.4"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.4.4"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is True
        assert result["pg_receivexlog_synchronous"] is False

    @patch("barman.wal_archiver.StreamingWalArchiver.get_remote_status")
    @patch("barman.wal_archiver.PgReceiveXlog")
    def test_receive_wal(self, receivexlog_mock, remote_mock, tmpdir):
        backup_manager = build_backup_manager(
            main_conf={"backup_directory": tmpdir},
        )
        streaming_mock = backup_manager.server.streaming
        streaming_mock.server_txt_version = "9.4.0"
        streaming_mock.get_connection_string.return_value = (
            "host=pg01.nowhere user=postgres port=5432 "
            "application_name=barman_receive_wal"
        )
        streaming_mock.get_remote_status.return_value = {
            "streaming_supported": True,
            "timeline": 1,
        }
        postgres_mock = backup_manager.server.postgres
        replication_slot_status = MagicMock(restart_lsn="F/A12D687", active=False)
        postgres_mock.get_remote_status.return_value = {
            "current_xlog": "000000010000000F0000000A",
            "current_lsn": "F/A12D687",
            "replication_slot": replication_slot_status,
            "xlog_segment_size": 16777216,
        }
        backup_manager.server.streaming.conn_parameters = {
            "host": "pg01.nowhere",
            "user": "postgres",
            "port": "5432",
        }
        streaming_dir = tmpdir.join("streaming")
        streaming_dir.ensure(dir=True)
        # Test: normal run
        archiver = StreamingWalArchiver(backup_manager)
        archiver.server.streaming.server_version = 90400
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": True,
            "pg_receivexlog_synchronous": None,
            "pg_receivexlog_path": "fake/path",
            "pg_receivexlog_supports_slots": True,
            "pg_receivexlog_version": "9.4",
        }

        # Test: execute a reset request
        partial = streaming_dir.join("000000010000000100000001.partial")
        partial.ensure()
        archiver.receive_wal(reset=True)
        assert not partial.check()
        assert streaming_dir.join("000000010000000F0000000A.partial").check()

        archiver.receive_wal(reset=False)
        receivexlog_mock.assert_called_once_with(
            app_name="barman_receive_wal",
            synchronous=None,
            connection=ANY,
            destination=streaming_dir.strpath,
            err_handler=ANY,
            out_handler=ANY,
            path=ANY,
            slot_name=None,
            command="fake/path",
            version="9.4",
        )
        receivexlog_mock.return_value.execute.assert_called_once_with()

        # Test: pg_receivexlog from 9.2
        receivexlog_mock.reset_mock()
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": True,
            "pg_receivexlog_synchronous": False,
            "pg_receivexlog_path": "fake/path",
            "pg_receivexlog_supports_slots": False,
            "pg_receivexlog_version": "9.2",
        }
        archiver.receive_wal(reset=False)
        receivexlog_mock.assert_called_once_with(
            app_name="barman_receive_wal",
            synchronous=False,
            connection=ANY,
            destination=streaming_dir.strpath,
            err_handler=ANY,
            out_handler=ANY,
            path=ANY,
            command="fake/path",
            slot_name=None,
            version="9.2",
        )
        receivexlog_mock.return_value.execute.assert_called_once_with()

        # Test: incompatible pg_receivexlog
        with pytest.raises(ArchiverFailure):
            remote_mock.return_value = {
                "pg_receivexlog_installed": True,
                "pg_receivexlog_compatible": False,
                "pg_receivexlog_supports_slots": False,
                "pg_receivexlog_synchronous": False,
                "pg_receivexlog_path": "fake/path",
            }
            archiver.receive_wal()

        # Test: missing pg_receivexlog
        with pytest.raises(ArchiverFailure):
            remote_mock.return_value = {
                "pg_receivexlog_installed": False,
                "pg_receivexlog_compatible": True,
                "pg_receivexlog_supports_slots": False,
                "pg_receivexlog_synchronous": False,
                "pg_receivexlog_path": "fake/path",
            }
            archiver.receive_wal()
        # Test: impossible to connect with streaming protocol
        with pytest.raises(ArchiverFailure):
            backup_manager.server.streaming.get_remote_status.return_value = {
                "streaming_supported": None
            }
            remote_mock.return_value = {
                "pg_receivexlog_installed": True,
                "pg_receivexlog_supports_slots": False,
                "pg_receivexlog_compatible": True,
                "pg_receivexlog_synchronous": False,
                "pg_receivexlog_path": "fake/path",
            }
            archiver.receive_wal()
        # Test: PostgreSQL too old
        with pytest.raises(ArchiverFailure):
            backup_manager.server.streaming.get_remote_status.return_value = {
                "streaming_supported": False
            }
            remote_mock.return_value = {
                "pg_receivexlog_installed": True,
                "pg_receivexlog_compatible": True,
                "pg_receivexlog_synchronous": False,
                "pg_receivexlog_path": "fake/path",
            }
            archiver.receive_wal()
        # Test: general failure executing pg_receivexlog
        with pytest.raises(ArchiverFailure):
            remote_mock.return_value = {
                "pg_receivexlog_installed": True,
                "pg_receivexlog_compatible": True,
                "pg_receivexlog_synchronous": False,
                "pg_receivexlog_path": "fake/path",
            }
            receivexlog_mock.return_value.execute.side_effect = CommandFailedException
            archiver.receive_wal()

    @patch("barman.utils.which")
    @patch("barman.command_wrappers.Command")
    def test_when_streaming_connection_rejected(self, command_mock, which_mock):
        """
        Test the StreamingWalArchiver behaviour when the streaming
        connection is rejected by the PostgreSQL server and
        pg_receivexlog is installed.
        """

        # When the streaming connection is not available, the
        # server_txt_version property will have a None value.
        backup_manager = build_backup_manager()
        backup_manager.server.streaming.server_major_version = None
        archiver = StreamingWalArchiver(backup_manager)
        which_mock.return_value = "/some/path/to/pg_receivexlog"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.2"

        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is None

    @patch("barman.wal_archiver.StreamingWalArchiver.get_remote_status")
    def test_check(self, remote_mock, capsys):
        """
        Test management of check_postgres view output

        :param remote_mock: mock get_remote_status function
        :param capsys: retrieve output from console
        """
        # Create a backup_manager
        backup_manager = build_backup_manager()
        # Set up mock responses
        streaming = backup_manager.server.streaming
        streaming.server_txt_version = "9.5"
        # Instantiate a StreamingWalArchiver obj
        archiver = StreamingWalArchiver(backup_manager)
        # Prepare the output check strategy
        strategy = CheckOutputStrategy()
        # Case: correct configuration
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": True,
            "pg_receivexlog_path": "fake/path",
            "incoming_wals_count": 0,
        }
        # Expect out: all parameters: OK
        backup_manager.server.process_manager.list.return_value = []
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: OK\n"
            "\treceive-wal running: FAILED "
            "(See the Barman log file for more details)\n"
        )

        # Case: pg_receivexlog is not compatible
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": False,
            "pg_receivexlog_path": "fake/path",
            "pg_receivexlog_version": "9.2",
            "incoming_wals_count": 0,
        }
        # Expect out: some parameters: FAILED
        strategy = CheckOutputStrategy()
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: FAILED "
            "(PostgreSQL version: 9.5, pg_receivexlog version: 9.2)\n"
            "\treceive-wal running: FAILED "
            "(See the Barman log file for more details)\n"
        )
        # Case: pg_receivexlog returned error
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": None,
            "pg_receivexlog_path": "fake/path",
            "pg_receivexlog_version": None,
            "incoming_wals_count": 0,
        }
        # Expect out: all parameters: OK
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: FAILED "
            "(PostgreSQL version: 9.5, pg_receivexlog version: None)\n"
            "\treceive-wal running: FAILED "
            "(See the Barman log file for more details)\n"
        )

        # Case: receive-wal running
        backup_manager.server.process_manager.list.return_value = [
            ProcessInfo(
                pid=1, server_name=backup_manager.config.name, task="receive-wal"
            )
        ]
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: FAILED "
            "(PostgreSQL version: 9.5, pg_receivexlog version: None)\n"
            "\treceive-wal running: OK\n"
        )

        # Case: streaming connection not configured
        backup_manager.server.streaming = None
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: FAILED "
            "(PostgreSQL version: Unknown, pg_receivexlog version: None)\n"
            "\treceive-wal running: OK\n"
        )
        # Case: too many wal files in the incoming queue
        archiver.config.max_incoming_wals_queue = 10
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": None,
            "pg_receivexlog_path": "fake/path",
            "pg_receivexlog_version": None,
            "incoming_wals_count": 20,
        }
        # Expect out: the wals incoming queue is too big
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: FAILED "
            "(PostgreSQL version: Unknown, pg_receivexlog version: None)\n"
            "\treceive-wal running: OK\n"
        )

    @patch("barman.wal_archiver.glob")
    @patch("os.path.exists")
    @patch("os.path.isfile")
    @patch("barman.wal_archiver.WalFileInfo.from_file")
    def test_get_next_batch(
        self, from_file_mock, isfile_mock, exists_mock, glob_mock, caplog
    ):
        """
        Test the FileWalArchiver.get_next_batch method
        """
        # See all logs
        caplog.set_level(0)

        # WAL batch, with 000000010000000000000001 that is currently being
        # written
        glob_mock.return_value = ["000000010000000000000001"]
        isfile_mock.return_value = True
        # This is an hack, instead of a WalFileInfo we use a simple string to
        # ease all the comparisons. The resulting string is the name enclosed
        # in colons. e.g. ":000000010000000000000001:"
        from_file_mock.side_effect = (
            lambda filename, compression_manager, unidentified_compression, encryption_manager, *args, **kwargs: ":%s:"
            % filename
        )

        backup_manager = build_backup_manager(name="TestServer")
        archiver = StreamingWalArchiver(backup_manager)
        backup_manager.server.archivers = [archiver]

        caplog_reset(caplog)
        batch = archiver.get_next_batch()
        assert ["000000010000000000000001"] == batch.skip
        assert "" == caplog.text

        # WAL batch, with 000000010000000000000002 that is currently being
        # written and 000000010000000000000001 can be archived
        caplog_reset(caplog)
        glob_mock.return_value = [
            "000000010000000000000001",
            "000000010000000000000002",
        ]
        batch = archiver.get_next_batch()
        assert [":000000010000000000000001:"] == batch
        assert ["000000010000000000000002"] == batch.skip
        assert "" == caplog.text

        # WAL batch, with two partial files.
        caplog_reset(caplog)
        glob_mock.return_value = [
            "000000010000000000000001.partial",
            "000000010000000000000002.partial",
        ]
        batch = archiver.get_next_batch()
        assert [":000000010000000000000001.partial:"] == batch
        assert ["000000010000000000000002.partial"] == batch.skip
        assert (
            "Archiving partial files for server %s: "
            "000000010000000000000001.partial" % archiver.config.name
        ) in caplog.text

        # WAL batch, with history files.
        caplog_reset(caplog)
        glob_mock.return_value = [
            "00000001.history",
            "000000010000000000000002.partial",
        ]
        batch = archiver.get_next_batch()
        assert [":00000001.history:"] == batch
        assert ["000000010000000000000002.partial"] == batch.skip
        assert "" == caplog.text

        # WAL batch with errors
        wrong_file_name = "test_wrong_wal_file.2"
        glob_mock.return_value = ["test_wrong_wal_file.2"]
        batch = archiver.get_next_batch()
        assert [wrong_file_name] == batch.errors

        # WAL batch, with two partial files, but one has been just renamed.
        caplog_reset(caplog)
        exists_mock.side_effect = [False, True]
        glob_mock.return_value = [
            "000000010000000000000001.partial",
            "000000010000000000000002.partial",
        ]
        batch = archiver.get_next_batch()
        assert len(batch) == 0
        assert ["000000010000000000000002.partial"] == batch.skip
        assert "" in caplog.text

    def test_is_synchronous(self):
        backup_manager = build_backup_manager(name="TestServer")
        archiver = StreamingWalArchiver(backup_manager)

        # 'barman_receive_wal' is not in the list of synchronous standby
        # names, so we expect is_synchronous to be false
        backup_manager.server.postgres.get_remote_status.return_value = {
            "synchronous_standby_names": ["a", "b", "c"]
        }
        assert not archiver._is_synchronous()

        # 'barman_receive_wal' is in the list of synchronous standby
        # names, so we expect is_synchronous to be true
        backup_manager.server.postgres.get_remote_status.return_value = {
            "synchronous_standby_names": ["a", "barman_receive_wal"]
        }
        assert archiver._is_synchronous()

        # '*' is in the list of synchronous standby names, so we expect
        # is_synchronous to be true even if 'barman_receive_wal' is not
        # explicitly referenced
        backup_manager.server.postgres.get_remote_status.return_value = {
            "synchronous_standby_names": ["a", "b", "*"]
        }
        assert archiver._is_synchronous()

        # There is only a '*' in the list of synchronous standby names,
        # so we expect every name to match
        backup_manager.server.postgres.get_remote_status.return_value = {
            "synchronous_standby_names": ["*"]
        }
        assert archiver._is_synchronous()


class TestWalStorageStrategy:
    """
    Tests for the :class:`WalStorageStrategy` abstract class.

    .. note::
        As :class:`WalStorageStrategy` is an abstract class, we need to
        patch the ``__abstractmethods__`` attribute in most tests to be able to
        instantiate it.
    """

    @patch("barman.wal_archiver.RetryHookScriptRunner")
    @patch("barman.wal_archiver.HookScriptRunner")
    @patch(
        "barman.wal_archiver.WalStorageStrategy.__abstractmethods__", new_callable=set
    )
    def test_run_pre_archive_scripts(self, _, mock_hook_script, mock_retry_hook_script):
        """Test that the pre-archive scripts are run correctly when present"""
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = WalStorageStrategy(backup_manager, backup_manager.server)

        # WHEN _run_pre_archive_scripts is called
        arg_wal_info, arg_src_path = MagicMock(), "/mock/src/path"
        wal_storage._run_pre_archive_scripts(arg_wal_info, arg_src_path)

        # THEN the pre-archive hook script is instantiated and run correctly
        mock_hook_script.assert_called_once_with(
            backup_manager, "archive_script", "pre"
        )
        mock_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info, arg_src_path
        )
        mock_hook_script.return_value.run.assert_called_once()

        # AND the pre-archive retry hook script is instantiated and run correctly
        mock_retry_hook_script.assert_called_once_with(
            backup_manager, "archive_retry_script", "pre"
        )
        mock_retry_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info, arg_src_path
        )
        mock_retry_hook_script.return_value.run.assert_called_once()

    @patch("barman.wal_archiver.RetryHookScriptRunner")
    @patch("barman.wal_archiver.HookScriptRunner")
    @patch(
        "barman.wal_archiver.WalStorageStrategy.__abstractmethods__", new_callable=set
    )
    def test_run_post_archive_scripts(
        self, _, mock_hook_script, mock_retry_hook_script
    ):
        """Test that the post-archive scripts are run correctly when present"""
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = WalStorageStrategy(backup_manager, backup_manager.server)

        # WHEN _run_post_archive_scripts is called
        arg_wal_info, arg_src_path, arg_error = MagicMock(), "/mock/src/path", None
        wal_storage._run_post_archive_scripts(arg_wal_info, arg_src_path, arg_error)

        # THEN the post-archive retry hook script is instantiated and run correctly
        mock_retry_hook_script.assert_called_once_with(
            backup_manager, "archive_retry_script", "post"
        )
        mock_retry_hook_script.env_from_wal_info(arg_wal_info, arg_src_path, arg_error)
        mock_retry_hook_script.run()

        # AND the post-archive hook script is instantiated and run correctly
        mock_hook_script.assert_called_once_with(
            backup_manager, "archive_script", "post", arg_error
        )
        mock_hook_script.env_from_wal_info(arg_wal_info, arg_src_path)
        mock_hook_script.run()

    @patch("barman.wal_archiver._logger")
    @patch("barman.wal_archiver.RetryHookScriptRunner")
    @patch("barman.wal_archiver.HookScriptRunner")
    @patch(
        "barman.wal_archiver.WalStorageStrategy.__abstractmethods__", new_callable=set
    )
    def test_run_post_archive_scripts_with_error(
        self, _, mock_hook_script, mock_retry_hook_script, mock_logger
    ):
        """
        Test that the post-archive scripts are run correctly when an error occurred.

        That hapens when the retry hook script raises an exec:`AbortedRetryHookScript`
        exception.
        """
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = WalStorageStrategy(backup_manager, backup_manager.server)

        # Mock the retry hook script to raise AbortedRetryHookScript when run
        mock_retry_hook_script.return_value.run.side_effect = AbortedRetryHookScript(
            hook=mock_retry_hook_script.return_value
        )

        # WHEN _run_post_archive_scripts is called
        arg_wal_info, arg_src_path, arg_error = MagicMock(), "/mock/src/path", None
        wal_storage._run_post_archive_scripts(arg_wal_info, arg_src_path, arg_error)

        # THEN the post-archive retry hook script is instantiated and run correctly
        # AND the AbortedRetryHookScript exception is catched and logged
        mock_retry_hook_script.assert_called_once_with(
            backup_manager, "archive_retry_script", "post"
        )
        mock_retry_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info, arg_src_path, arg_error
        )
        mock_retry_hook_script.return_value.run.assert_called_once()
        mock_logger.warning.assert_called_once_with(
            "Ignoring stop request after receiving "
            "abort (exit code %d) from post-archive "
            "retry hook script: %s",
            mock_retry_hook_script.return_value.exit_status,
            mock_retry_hook_script.return_value.script,
        )

        # AND the post-archive hook script is instantiated and run correctly
        # regardless of the error in the previous retry hook script
        mock_hook_script.assert_called_once_with(
            backup_manager, "archive_script", "post", arg_error
        )
        mock_hook_script.env_from_wal_info(arg_wal_info, arg_src_path)
        mock_hook_script.run()


class TestLocalWalStorageStrategy:
    """Tests for the :class:`LocalWalStorageStrategy` class"""

    @patch("barman.wal_archiver.os.path.exists", return_value=False)
    def test_check_duplicate_file_not_exists(self, _):
        """
        Test that no exception is raised when there is no duplicate file
        at the destination path.
        """
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = LocalWalStorageStrategy(backup_manager, backup_manager.server)
        # WHEN _check_duplicate is called for a file that does not exist
        arg_src, arg_dest, arg_wal_info = "/src/path", "/dest/path", MagicMock()
        # THEN no exception is raised
        wal_storage._check_duplicate(arg_src, arg_dest, arg_wal_info)

    @patch("barman.wal_archiver.filecmp.cmp", return_value=False)
    @patch("barman.wal_archiver.os.path.exists", return_value=True)
    def test_check_duplicate_file_exists_with_different_content(self, _, mock_cmp):
        """
        Test that a exec:`DuplicateWalFile` exception is raised when a file
        with different content exists at the destination path.
        """
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = LocalWalStorageStrategy(backup_manager, backup_manager.server)
        # Mock the get_wal_file_info to return a wal_info representing a destination
        # file without compression and encryption as we're not testing that here
        backup_manager.get_wal_file_info = MagicMock(
            return_value=MagicMock(compression=None, encryption=None)
        )
        # Prepare the arguments for _check_duplicate
        # Mock the src wal_info to represent a source file without compression and encryption
        arg_src, arg_dest, arg_wal_info = (
            "/src/path",
            "/dest/path",
            MagicMock(compression=None, encryption=None),
        )

        # WHEN _check_duplicate is called
        # THEN a DuplicateWalFile exception is raised, as the contents are different
        with pytest.raises(DuplicateWalFile):
            wal_storage._check_duplicate(arg_src, arg_dest, arg_wal_info)
            # Also assert that filecmp.cmp was called with the correct arguments
            mock_cmp.assert_called_once_with(arg_src, arg_dest)

    @patch("barman.wal_archiver.filecmp.cmp", return_value=True)
    @patch("barman.wal_archiver.os.path.exists", return_value=True)
    def test_check_duplicate_file_exists_with_same_content(self, _, mock_cmp):
        """
        Test that a exec:`MatchingDuplicateWalFile` exception is raised when a file
        with the same content exists at the destination path.
        """
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = LocalWalStorageStrategy(backup_manager, backup_manager.server)
        # Mock the get_wal_file_info to return a wal_info representing a destination
        # file without compression and encryption as we're not testing that here
        backup_manager.get_wal_file_info = MagicMock(
            return_value=MagicMock(compression=None, encryption=None)
        )
        # Prepare the arguments for _check_duplicate
        # Mock the src wal_info to represent a source file without compression and encryption
        arg_src, arg_dest, arg_wal_info = (
            "/src/path",
            "/dest/path",
            MagicMock(compression=None, encryption=None),
        )

        # WHEN _check_duplicate is called
        # THEN a MatchingDuplicateWalFile exception is raised, as the contents are the same
        with pytest.raises(MatchingDuplicateWalFile):
            wal_storage._check_duplicate(arg_src, arg_dest, arg_wal_info)
            # Also assert that filecmp.cmp was called with the correct arguments
            mock_cmp.assert_called_once_with(arg_src, arg_dest)

    @patch("barman.wal_archiver.os.path.exists", return_value=True)
    def test_check_duplicate_file_exists_with_encryption(self, _):
        """
        Test that a exec:`DuplicateWalFile` exception is raised when a file
        with encryption exists at the destination path.
        """
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = LocalWalStorageStrategy(backup_manager, backup_manager.server)
        # Mock the get_wal_file_info to return a wal_info representing a destination
        # file with encryption
        backup_manager.get_wal_file_info = MagicMock(
            return_value=MagicMock(compression=None, encryption="gpg")
        )
        # Mock the src wal_info to represent a source file with encryption
        arg_src, arg_dest, arg_wal_info = (
            "/src/path",
            "/dest/path",
            MagicMock(compression=None, encryption="gpg"),
        )

        # WHEN _check_duplicate is called
        # THEN a DuplicateWalFile exception is raised, as we cannot compare encrypted files
        with pytest.raises(DuplicateWalFile):
            wal_storage._check_duplicate(arg_src, arg_dest, arg_wal_info)

    @pytest.mark.parametrize("cmp_result", [True, False])
    @patch("barman.wal_archiver.os.unlink")
    @patch("barman.wal_archiver.filecmp.cmp")
    @patch("barman.wal_archiver.os.path.exists", return_value=True)
    def test_check_duplicate_file_exists_with_compression(
        self, _, mock_unlink, mock_cmp, cmp_result
    ):
        """
        Test that compressed files are decompressed correctly before comparison.

        Note that the exact exception raised is not the focus here.
        """
        mock_cmp.return_value = cmp_result
        backup_manager = build_backup_manager(name="TestServer")
        backup_manager.compression_manager = MagicMock()
        wal_storage = LocalWalStorageStrategy(backup_manager, backup_manager.server)
        # Mock the get_wal_file_info to return a wal_info representing a destination
        # file with compression
        backup_manager.get_wal_file_info = MagicMock(
            return_value=MagicMock(compression="gzip", encryption=None)
        )
        # Prepare the arguments for _check_duplicate
        # Mock the src wal_info to represent a source file with compression
        arg_src, arg_dest, arg_wal_info = (
            "/src/path",
            "/dest/path",
            MagicMock(compression="gzip", encryption=None),
        )
        # Prepare the expected temporary uncompressed file paths
        src_uncompressed, dest_uncompressed = "/src/uncompressed", "/dest/uncompressed"

        # WHEN _check_duplicate is called
        # THEN the appropriate exception is raised, according to the result of filecmp.cmp
        # Note: we don't care about the exact exception raised here, the important part
        # is ensuring that the decompression and comparison were done correctly
        ex = MatchingDuplicateWalFile if cmp_result else DuplicateWalFile
        with pytest.raises(ex):
            wal_storage._check_duplicate(arg_src, arg_dest, arg_wal_info)
            # AND the compressed src and dst files were decompressed correctly
            backup_manager.compression_manager.get_compressor.return_value.decompress.assert_called_once_with(
                arg_dest, dest_uncompressed
            )
            backup_manager.compression_manager.get_compressor.return_value.decompress.assert_called_once_with(
                arg_dest, src_uncompressed
            )
            # AND filecmp.cmp was called with the correct arguments
            mock_cmp.assert_called_once_with(src_uncompressed, dest_uncompressed)
            # AND the temporary uncompressed files were removed
            mock_unlink.assert_called_once_with(src_uncompressed)
            mock_unlink.assert_called_once_with(dest_uncompressed)

    def test_compress_file(self):
        """
        Test that :meth:`compress_file` correctly compresses and returns a file.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        mock_compressor = MagicMock(compression="gzip")
        mock_wal_info = MagicMock()

        # WHEN _compress_file is called
        result = wal_storage._compress_file(
            mock_compressor, "/src/000000010000000000000001", "/dest/dir", mock_wal_info
        )
        # THEN the compressor's compress method is called with correct arguments
        mock_compressor.compress.assert_called_once_with(
            "/src/000000010000000000000001",
            "/dest/dir/000000010000000000000001.compressed",
        )
        # AND the previous source file is appended to the removal list
        assert wal_storage.files_to_remove == ["/src/000000010000000000000001"]
        # AND the compression method is set in the wal_info object
        assert mock_wal_info.compression == "gzip"
        # AND the correct compressed file path is returned
        assert result == "/dest/dir/000000010000000000000001.compressed"

    def test_encrypt_file(self):
        """
        Test that :meth:`encrypt_file` correctly encrypts and returns a file.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        mock_encryption = MagicMock(NAME="gpg")
        mock_wal_info = MagicMock()

        # WHEN _encrypt_file is called
        result = wal_storage._encrypt_file(
            mock_encryption,
            "/src/000000010000000000000001",
            "/dest/dir",
            mock_wal_info,
        )
        # THEN the encryption's encrypt method is called with correct arguments
        mock_encryption.encrypt.assert_called_once_with(
            "/src/000000010000000000000001",
            "/dest/dir",
        )
        # AND the previous source file is appended to the removal list
        assert wal_storage.files_to_remove == ["/src/000000010000000000000001"]
        # AND the encryption method is set in the wal_info object
        assert mock_wal_info.encryption == "gpg"
        # AND the encrypted file path is returned
        assert result == mock_encryption.encrypt.return_value

    @patch("barman.wal_archiver.os.stat")
    @patch("barman.wal_archiver.shutil.copystat")
    def test_copy_stats(self, mock_copystat, mock_stat):
        """
        Test that :meth:`copy_stats` correctly copies file stats from the source
        file to the current file and updates the ``wal_info`` size stats.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        mock_wal_info = MagicMock()
        src_file, current_file = "/src/path/file1", "/dest/path/file2"

        # WHEN _copy_stats is called
        wal_storage._copy_stats(src_file, current_file, mock_wal_info)
        # THEN the file stats are copied from the source to the current file
        mock_copystat.assert_called_once_with(src_file, current_file)
        # AND the wal_info size is updated with the size of the current file
        mock_stat.assert_called_with(current_file)
        assert mock_wal_info.size == mock_stat.return_value.st_size

    @patch("barman.wal_archiver.fsync_dir")
    @patch("barman.wal_archiver.fsync_file")
    def test_fsync_contents(self, mock_fsync_file, mock_fsync_dir):
        """
        Test that :meth:`_fsync_contents` fsyncs the destination file
        and the source and destination directories.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        src_dir, dest_dir, dest_file = "/src/dir", "/dest/dir", "/dest/dir/walfile"

        # WHEN _fsync_contents is called
        wal_storage._fsync_contents(src_dir, dest_dir, dest_file)
        # THEN the destination file is fsynced
        mock_fsync_file.assert_called_once_with(dest_file)
        # AND the src and dest directories is fsynced
        mock_fsync_dir.assert_has_calls([call(src_dir), call(dest_dir)])

    @patch("barman.wal_archiver.os.rename")
    def test_rename_or_copy_file(self, mock_rename):
        """
        Test that :meth:`_rename_or_copy_file` correctly renames
        the source file to the destination file when they are on the same filesystem.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        file, dest_file = "/src/path/file1", "/dest/path/file2"
        # WHEN _rename_or_copy_file is called
        wal_storage._rename_or_copy_file(file, dest_file)
        # THEN os.rename is called to move the file
        mock_rename.assert_called_once_with(file, dest_file)

    @patch("barman.wal_archiver.shutil.copy2")
    @patch("barman.wal_archiver.os.rename", side_effect=OSError)
    def test_rename_or_copy_file_different_fs(self, _, mock_copy2):
        """
        Test that :meth:`_rename_or_copy_file` correctly copies the source file
        to the destination file when they are on different filesystems.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        file, dest_file = "/src/path/file1", "/dest/path/file2"
        # WHEN _rename_or_copy_file is called and os.rename raises OSError
        wal_storage._rename_or_copy_file(file, dest_file)
        # THEN shutil.copy2 is called to copy the file as a fallback
        mock_copy2.assert_called_once_with(file, dest_file)
        # AND the previous source file is appended to the removal list
        assert wal_storage.files_to_remove == [file]

    @patch("barman.wal_archiver.os.unlink")
    def test_remove_intermediary_files(self, mock_unlink):
        """
        Test that :meth:`_remove_intermediary_files` correctly removes
        all intermediary files listed in `files_to_remove`.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        wal_storage.files_to_remove = [
            "/path/to/tempfile1",
            "/path/to/tempfile2",
            "/path/to/tempfile3",
        ]
        # WHEN _remove_intermediary_files is called
        wal_storage._remove_intermediary_files()
        # THEN os.unlink is called for each file in the removal list
        mock_unlink.assert_has_calls(
            [
                call("/path/to/tempfile1"),
                call("/path/to/tempfile2"),
                call("/path/to/tempfile3"),
            ]
        )

    @patch("barman.wal_archiver.mkpath")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._copy_stats")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._compress_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._encrypt_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._rename_or_copy_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._remove_intermediary_files")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._fsync_contents")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_post_archive_scripts")
    def test_save(
        self,
        mock_run_post_scripts,
        mock_fsync,
        mock_remove,
        mock_rename,
        mock_encrypt,
        mock_compress,
        mock_copy_stats,
        mock_check_duplicate,
        mock_run_pre_scripts,
        mock_mkpath,
    ):
        """
        Test that :meth:`save` correctly manages the saving of a WAL file
        (without compression or encryption).
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        # AND no compression nor encryption is requested
        compressor, encryption = None, None
        # AND a mock WalFileInfo object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            fullpath=lambda x: "/server/wals/000000010000000000000001",
            compression=None,
            encryption=None,
        )
        # WHEN save is called
        wal_storage.save(compressor, encryption, mock_wal_info)
        # THEN ensure the destination directory is created
        mock_mkpath.assert_called_once_with("/server/wals")
        # AND the pre-archive scripts are run with correct arguments
        mock_run_pre_scripts.assert_called_once_with(
            mock_wal_info, "/src/path/000000010000000000000001"
        )
        # AND duplicate check is performed with correct arguments
        mock_check_duplicate.assert_called_once_with(
            "/src/path/000000010000000000000001",
            "/server/wals/000000010000000000000001",
            mock_wal_info,
        )
        # AND the xlogdb is opened for the save operation
        wal_storage.server.xlogdb.assert_called_once_with("a")
        wal_storage.server.xlogdb.return_value.__enter__.assert_called_once()
        # AND the file is renamed to the destination
        mock_rename.assert_called_once_with(
            "/src/path/000000010000000000000001",
            "/server/wals/000000010000000000000001",
        )
        # AND intermediary files are removed
        mock_remove.assert_called_once()
        # AND the contents are fsynced
        mock_fsync.assert_called_once_with(
            "/src/path", "/server/wals", "/server/wals/000000010000000000000001"
        )
        # AND the wal_info line is written to the opened xlogdb
        wal_storage.server.xlogdb.return_value.__enter__.return_value.write.assert_called_once_with(
            mock_wal_info.to_xlogdb_line.return_value
        )
        # AND finally the post-archive scripts are run with correct arguments
        mock_run_post_scripts.assert_called_once_with(
            mock_wal_info, "/server/wals/000000010000000000000001", None
        )
        # Lasly, ensure that no compression nor encryption were performed and, as
        # a consequence, not stats were updated
        mock_compress.assert_not_called()
        mock_encrypt.assert_not_called()
        mock_copy_stats.assert_not_called()

    @patch("barman.wal_archiver.mkpath")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._copy_stats")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._compress_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._encrypt_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._rename_or_copy_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._remove_intermediary_files")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._fsync_contents")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_post_archive_scripts")
    def test_save_with_compression(
        self,
        mock_run_post_scripts,
        mock_fsync,
        mock_remove,
        mock_rename,
        mock_encrypt,
        mock_compress,
        mock_copy_stats,
        mock_check_duplicate,
        mock_run_pre_scripts,
        mock_mkpath,
    ):
        """
        Test that :meth:`save` correctly manages the saving of a WAL file
        with compression but no encryption.
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        # AND compression as gzip and no encryption is requested
        compressor, encryption = MagicMock(compression="gzip"), None
        # AND a mock WalFileInfo object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            fullpath=lambda x: "/server/wals/000000010000000000000001",
            compression=None,
            encryption=None,
        )
        # WHEN save is called
        wal_storage.save(compressor, encryption, mock_wal_info)
        # THEN ensure the destination directory is created
        mock_mkpath.assert_called_once_with("/server/wals")
        # AND the pre-archive scripts are run with correct arguments
        mock_run_pre_scripts.assert_called_once_with(
            mock_wal_info, "/src/path/000000010000000000000001"
        )
        # AND duplicate check is performed with correct arguments
        mock_check_duplicate.assert_called_once_with(
            "/src/path/000000010000000000000001",
            "/server/wals/000000010000000000000001",
            mock_wal_info,
        )
        # AND _compress_file is called to compress the file
        mock_compress.assert_called_once_with(
            compressor,
            "/src/path/000000010000000000000001",
            "/server/wals",
            mock_wal_info
        )
        # AND the stats from the source file are updated to the compressed file
        mock_copy_stats.assert_called_once_with(
            "/src/path/000000010000000000000001",
            mock_compress.return_value
        )
        # AND the xlogdb is opened for the save operation
        wal_storage.server.xlogdb.assert_called_once_with("a")
        wal_storage.server.xlogdb.return_value.__enter__.assert_called_once()
        # AND the compressed file is renamed to the destination
        mock_rename.assert_called_once_with(
            mock_compress.return_value,
            "/server/wals/000000010000000000000001",
        )
        # AND intermediary files are removed
        mock_remove.assert_called_once()
        # AND the contents are fsynced
        mock_fsync.assert_called_once_with(
            "/src/path", "/server/wals", "/server/wals/000000010000000000000001"
        )
        # AND the wal_info line is written to the opened xlogdb
        wal_storage.server.xlogdb.return_value.__enter__.return_value.write.assert_called_once_with(
            mock_wal_info.to_xlogdb_line.return_value
        )
        # AND finally the post-archive scripts are run with correct arguments
        mock_run_post_scripts.assert_called_once_with(
            mock_wal_info, "/server/wals/000000010000000000000001", None
        )
        # Lasly, ensure that no encryption was performed
        mock_encrypt.assert_not_called()

    @patch("barman.wal_archiver.mkpath")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._copy_stats")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._compress_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._encrypt_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._rename_or_copy_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._remove_intermediary_files")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._fsync_contents")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_post_archive_scripts")
    def test_save_with_encryption(
        self,
        mock_run_post_scripts,
        mock_fsync,
        mock_remove,
        mock_rename,
        mock_encrypt,
        mock_compress,
        mock_copy_stats,
        mock_check_duplicate,
        mock_run_pre_scripts,
        mock_mkpath,
    ):
        """
        Test that :meth:`save` correctly manages the saving of a WAL file
        with encryption but no compression.
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        # AND no compression and encryption as gpg is requested
        compressor, encryption = None, MagicMock(NAME="gpg")
        # AND a mock WalFileInfo object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            fullpath=lambda x: "/server/wals/000000010000000000000001",
            compression=None,
            encryption=None,
        )
        # WHEN save is called
        wal_storage.save(compressor, encryption, mock_wal_info)
        # THEN ensure the destination directory is created
        mock_mkpath.assert_called_once_with("/server/wals")
        # AND the pre-archive scripts are run with correct arguments
        mock_run_pre_scripts.assert_called_once_with(
            mock_wal_info, "/src/path/000000010000000000000001"
        )
        # AND duplicate check is performed with correct arguments
        mock_check_duplicate.assert_called_once_with(
            "/src/path/000000010000000000000001",
            "/server/wals/000000010000000000000001",
            mock_wal_info,
        )
        # AND _encrypt_file is called to encrypt the file
        mock_encrypt.assert_called_once_with(
            encryption,
            "/src/path/000000010000000000000001",
            "/server/wals",
            mock_wal_info
        )
        # AND the stats from the source file are updated to the encrypted file
        mock_copy_stats.assert_called_once_with(
            "/src/path/000000010000000000000001",
            mock_encrypt.return_value
        )
        # AND the xlogdb is opened for the save operation
        wal_storage.server.xlogdb.assert_called_once_with("a")
        wal_storage.server.xlogdb.return_value.__enter__.assert_called_once()
        # AND the encrypted file is renamed to the destination
        mock_rename.assert_called_once_with(
            mock_encrypt.return_value,
            "/server/wals/000000010000000000000001",
        )
        # AND intermediary files are removed
        mock_remove.assert_called_once()
        # AND the contents are fsynced
        mock_fsync.assert_called_once_with(
            "/src/path", "/server/wals", "/server/wals/000000010000000000000001"
        )
        # AND the wal_info line is written to the opened xlogdb
        wal_storage.server.xlogdb.return_value.__enter__.return_value.write.assert_called_once_with(
            mock_wal_info.to_xlogdb_line.return_value
        )
        # AND finally the post-archive scripts are run with correct arguments
        mock_run_post_scripts.assert_called_once_with(
            mock_wal_info, "/server/wals/000000010000000000000001", None
        )
        # Lasly, ensure that no compression was performed
        mock_compress.assert_not_called()


    @patch("barman.wal_archiver.mkpath")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._copy_stats")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._compress_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._encrypt_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._rename_or_copy_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._remove_intermediary_files")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._fsync_contents")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_post_archive_scripts")
    def test_save_with_compression_and_encryption(
        self,
        mock_run_post_scripts,
        mock_fsync,
        mock_remove,
        mock_rename,
        mock_encrypt,
        mock_compress,
        mock_copy_stats,
        mock_check_duplicate,
        mock_run_pre_scripts,
        mock_mkpath,
    ):
        """
        Test that :meth:`save` correctly manages the saving of a WAL file
        with both compression and encryption.
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        # AND compression as gzip and encryption as gpg is requested
        compressor, encryption = MagicMock(compression="gzip"), MagicMock(NAME="gpg")
        # AND a mock WalFileInfo object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            fullpath=lambda x: "/server/wals/000000010000000000000001",
            compression=None,
            encryption=None,
        )
        # WHEN save is called
        wal_storage.save(compressor, encryption, mock_wal_info)
        # THEN ensure the destination directory is created
        mock_mkpath.assert_called_once_with("/server/wals")
        # AND the pre-archive scripts are run with correct arguments
        mock_run_pre_scripts.assert_called_once_with(
            mock_wal_info, "/src/path/000000010000000000000001"
        )
        # AND duplicate check is performed with correct arguments
        mock_check_duplicate.assert_called_once_with(
            "/src/path/000000010000000000000001",
            "/server/wals/000000010000000000000001",
            mock_wal_info,
        )
        # AND _compress_file is called to compress the file
        mock_compress.assert_called_once_with(
            compressor,
            "/src/path/000000010000000000000001",
            "/server/wals",
            mock_wal_info
        )
        # AND _encrypt_file is called to encrypt the compressed file
        mock_encrypt.assert_called_once_with(
            encryption,
            mock_compress.return_value,
            "/server/wals",
            mock_wal_info
        )
        # AND the stats from the source file are updated to the compressed-encrypted file
        mock_copy_stats.assert_called_once_with(
            "/src/path/000000010000000000000001",
            mock_encrypt.return_value
        )
        # AND the xlogdb is opened for the save operation
        wal_storage.server.xlogdb.assert_called_once_with("a")
        wal_storage.server.xlogdb.return_value.__enter__.assert_called_once()
        # AND the compressed-encrypted file is renamed to the destination
        mock_rename.assert_called_once_with(
            mock_encrypt.return_value,
            "/server/wals/000000010000000000000001",
        )
        # AND intermediary files are removed
        mock_remove.assert_called_once()
        # AND the contents are fsynced
        mock_fsync.assert_called_once_with(
            "/src/path", "/server/wals", "/server/wals/000000010000000000000001"
        )
        # AND the wal_info line is written to the opened xlogdb
        wal_storage.server.xlogdb.return_value.__enter__.return_value.write.assert_called_once_with(
            mock_wal_info.to_xlogdb_line.return_value
        )
        # AND finally the post-archive scripts are run with correct arguments
        mock_run_post_scripts.assert_called_once_with(
            mock_wal_info, "/server/wals/000000010000000000000001", None
        )
