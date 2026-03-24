.. _commands-barman-list-processes:

``barman list-processes``
""""""""""""""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text

    list-processes
        [ { -h | --help } ]
        SERVER_NAME

Description
^^^^^^^^^^^

The ``list-processes`` sub-command outputs all active subprocesses for a Postgres server.
It displays the process identifier (PID) and the corresponding barman task for each active
subprocess.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the Postgres server for which to list active subprocesses.

``-h`` / ``--help``
    Displays a help message and exits.