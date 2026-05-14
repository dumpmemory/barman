.. _user_guide-export-import:

Backup export and import
========================

.. _user_guide-export-import-overview:

Overview
--------

Barman can export an existing backup as a single self-contained tarball
and later import it back into the Barman catalog. This bridges Barman with
enterprise data-management workflows that rely on external archival
systems, for example writing long-term copies to tape or moving backups
across storage tiers as part of a Grandfather/Father/Son strategy.

The exchange format is the following: the export is a tarball of
files; the import only requires that the tarball is presented on disk
to the target Barman server. Barman does not integrate with any specific
external product: any system that can archive a file from disk and later
restore it back can carry an exported backup end-to-end.

The outer tarball can optionally be compressed with the supporetd
algorithms (``gzip``, ``bzip2`` or ``xz``). Compression applies
only to the outer tarball: data files and WAL segments inside are not
recompressed, and any compression they already carry (for example WAL
or backup compression configured on the source server) is preserved
as-is. On import, the compression format is detected automatically
from the tarball and decompression is handled transparently.

.. _user_guide-export-import-limitations:

Limitations
-----------

* Only full backups in ``DONE`` state can be exported or imported;
  incremental backup chains are currently not supported for this feature.
* The feature is supported only on Barman servers using local storage.
* The exported tarball is tied to the source cluster's ``systemid``;
  importing into a different cluster is not supported.
* The original tarball filename must be preserved through the
  archival chain and renaming it breaks the integrity check.
* Encryption is preserved transparently; encrypted backups are
  exported and imported as-is.

.. _user_guide-export-import-tarball-layout:

Tarball layout
--------------

The exported tarball contains everything Barman needs to re-register
and restore the backup:

.. code-block:: text

    backup-export-SERVER-BACKUP_ID-TIMESTAMP-CHECKSUM.tar[.gz|.bz2|.xz]
    ├── identity.json            # server identity
    ├── backup.info              # backup metadata
    ├── barman.json              # additional Barman metadata
    ├── backup/                  # backup data tree (PGDATA + tablespaces)
    ├── wals/<hash>/<wal_name>   # WAL segments required for consistency
    └── xlog.db                  # WAL metadata

The symlinks normally found in ``pg_tblspc/`` are not stored in the
tarball: tablespace data is stored as plain subdirectories and the
symlinks are reconstructed during import.

.. _user_guide-export-import-filename:

Tarball filename and integrity
------------------------------

The filename follows the pattern:

.. code-block:: text

    backup-export-{SERVER_NAME}-{BACKUP_ID}-{TIMESTAMP}-{CHECKSUM}.tar[.gz|.bz2|.xz]

where ``TIMESTAMP`` is the export time in UTC (``YYYYMMDDTHHMMSS``) and
``CHECKSUM`` is the first eight characters of the SHA-256 hash of the
whole tarball. When ``barman import-backup`` is run, it recomputes the
SHA-256 of the file and compares it against the value embedded in the
filename, refusing the import on mismatch.

.. warning::

   Do not rename the exported tarball when transferring it through
   your archival system. Renaming breaks the integrity check and
   ``barman import-backup`` will refuse it. If your archival system
   stores files under a different name, restore them to the original
   name before importing.

.. _user_guide-export-import-usage:

Usage
-----

The following walk-through covers a full lifecycle: exporting a
backup, handing it off to an external archival system, restoring it
back to disk, and importing it into a Barman server for recovery.

1. **Pick a full backup** in ``DONE`` state on the source server:

   .. code-block:: bash

       barman list-backups main
       barman show-backup main 20260513T101530

2. **Export the backup** to an existing directory. Optionally compress
   the outer tarball with ``--compression {gzip|bzip2|xz}`` and tune
   the level with ``--compression-level``:

   .. code-block:: bash

       barman export-backup main 20260513T101530 /var/tmp/barman-exports

   Barman writes a single file under ``/var/tmp/barman-exports`` whose
   name embeds the export timestamp and integrity checksum, for
   example::

       backup-export-main-20260513T101530-20260513T134207-9f3a1c20.tar

3. **Archive the tarball** with your external backup product. Treat
   the file as opaque; the only requirement is that the original
   filename is preserved through the archival chain.

4. **Restore the tarball back to disk** on the target Barman server
   using your external product, into any directory of your choice.

5. **Import the backup** into the target catalog:

   .. code-block:: bash

       barman import-backup main \
           /var/tmp/barman-imports/backup-export-main-20260513T101530-20260513T134207-9f3a1c20.tar

   The import is atomic: data is extracted into a staging directory
   under ``basebackups_directory``, verified end-to-end, and only then
   moved into place. If any step fails, the staging directory and any
   partial WAL changes are cleaned up automatically and the catalog
   is left untouched.

   On success, the backup is annotated with ``KEEP:STANDALONE`` so
   that retention policies will not remove it, and a warning is
   printed to make this visible. Use ``barman keep main BACKUP_ID
   --release`` if you want the imported backup to participate in
   retention again (see :ref:`commands-barman-keep`).

6. **Verify and restore** to confirm the imported backup is fully
   usable:

   .. code-block:: bash

       barman list-backups main
       barman show-backup main 20260513T101530
       barman restore main 20260513T101530 /var/lib/postgresql/recovered
