.. _commands-barman-cloud-wal-archive:

``barman cloud-wal-archive``
""""""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text

    cloud-wal-archive
        [ { -h | --help } ]
        SERVER_NAME WAL_PATH

Description
^^^^^^^^^^^

Push a WAL file from the local disk to a configured cloud object storage. This command
is intended to be used in the ``archive_command`` of a Postgres server when using the
``local-to-cloud`` backup method for taking base backups.

.. note::
    The compression algorithm and level used for the WAL files can be configured in the
    Barman server configuration file. If not specified, the WAL file will be uploaded
    without compression.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    The name of the Postgres server for which the WAL file is being archived.

``WAL_PATH``
    The value of the ``%p`` keyword (according to ``archive_command``).

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.
