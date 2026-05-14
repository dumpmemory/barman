.. _commands-barman-import-backup:

``barman import-backup``
""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text

    import-backup
        [ { -h | --help } ]
        SERVER_NAME INPUT_TARBALL

Description
^^^^^^^^^^^

Register a previously exported backup tarball back into the Barman catalog of
``SERVER_NAME``. The tarball is expected to be the output of
:ref:`commands-barman-export-backup` (or a file restored to disk verbatim by
an external archival system from such an export). After a successful import,
the backup is fully usable: it appears in :ref:`commands-barman-list-backups`,
its metadata is available via :ref:`commands-barman-show-backup`, and it can
be restored with :ref:`commands-barman-restore`.

Imported backups are automatically protected from retention by applying the
``KEEP:STANDALONE`` annotation (see :ref:`commands-barman-keep`). If you want
the imported backup to participate in retention again, remove the annotation
explicitly.

Import is performed atomically: the tarball is extracted into a staging
directory under ``basebackups_directory``, verified end-to-end, and only
then moved into place. If any step fails, the staging area is cleaned up
and the catalog and WAL archive are left untouched. See this section
:ref:`user_guide-export-import` for the full feature description.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``INPUT_TARBALL``
    Path to the export tarball to import.