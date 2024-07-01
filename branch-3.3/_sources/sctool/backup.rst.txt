Backup
------

The backup commands allow you to: create and update a backup (ad-hoc or scheduled), list the contents of a backup, and list the backups of a cluster.
You cannot initiate a backup without a cluster. Make sure you add a cluster (:ref:`cluster-add`) before initiating a backup.

.. _sctool-backup:

backup
======

.. datatemplate:yaml:: partials/sctool_backup.yaml
   :template: command.tmpl

.. _backup-validate:

backup validate
===============

.. datatemplate:yaml:: partials/sctool_backup_validate.yaml
   :template: command.tmpl

.. _backup-update:

backup update
=============

.. datatemplate:yaml:: partials/sctool_backup_update.yaml
   :template: command.tmpl

.. _backup-list:

backup list
===========

.. datatemplate:yaml:: partials/sctool_backup_list.yaml
   :template: command.tmpl

.. _backup-delete:

backup delete
=============

.. datatemplate:yaml:: partials/sctool_backup_delete.yaml
   :template: command.tmpl
