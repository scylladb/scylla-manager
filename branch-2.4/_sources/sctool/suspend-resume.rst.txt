Suspend & Resume
----------------

The suspend command stops execution of **all** tasks that are running on a cluster.
The tasks can be resumed using the resume command.
If you want to stop a specific task, use the :ref:`task stop command <task-stop>`.

.. _suspend:

suspend
=======

When the suspend command is executed:

* The running tasks are stopped.
* The scheduled tasks are canceled.
* The :ref:`sctool task list <task-list>` command shows no next activation time.
* Starting a task manually fails.

The health check tasks are an exception and they run even after suspend.

**Syntax:**

.. code-block:: none

   sctool suspend [global flags]

suspend parameters
..................

suspend takes the :ref:`global-flags`.


.. _resume:

resume
======

The resume command reschedules the suspended tasks.
You can resume with the ``--start-tasks`` flag to start tasks that were stopped by the suspend command.

**Syntax:**

.. code-block:: none

   sctool resume [global flags]

resume parameters
..................

In addition to :ref:`global-flags`, resume takes the following parameters:

=====

.. _resume-start-tasks:

``--start-tasks``
^^^^^^^^^^^^^^^^^

Start tasks that were stopped by the suspend command.
