.. _commands-barman-cloud-wal-restore:

``barman cloud-wal-restore``
""""""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text

    cloud-wal-restore
        [ { -h | --help } ]
        [ { -p | --parallel } PARALLEL ]
        [ --spool-dir SPOOL_DIR ]
        SERVER_NAME WAL_NAME WAL_DEST

Description
^^^^^^^^^^^

Pull a WAL file from a configured cloud object storage to the local disk. This command
is intended to be used as ``restore_command`` in Postgres when restoring WALs from a
cloud object storage.

This command requires the server to have cloud storage configured via ``wals_directory``
pointing to a cloud URL (e.g., ``s3://barman/wals``).

Parameters
^^^^^^^^^^

``SERVER_NAME``
    The name of the Postgres server for which the WAL file is being restored.

``WAL_NAME``
    The value of the ``%f`` keyword (according to ``restore_command``).

``WAL_DEST``
    The value of the ``%p`` keyword (according to ``restore_command``).

``-h`` / ``--help``
    Show a help message and exit.

``-p`` / ``--parallel``
    Specifies the number of WAL files to peek and download in parallel. When set to a
    value greater than ``1``, fetches the requested WAL file along with the next
    ``N - 1`` files simultaneously. The additional files are staged in the spool
    directory so that subsequent restore requests can be served immediately from local
    storage. Default is ``0`` (disabled).

``--spool-dir``
    Directory used for staging extra WALs fetched when using ``--parallel``. Default is
    ``/var/tmp/walrestore``.

.. tip::
    When using ``--parallel`` and both the ``pg_wal`` directory and the spool directory
    are on the same filesystem, WAL files are served faster because they are renamed
    rather than copied.
