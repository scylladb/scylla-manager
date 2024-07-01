=======================================
Scylla Manager API online documentation
=======================================

Scylla Manager Server ships with `Swagger UI <https://swagger.io/tools/swagger-ui/>`_ that is served under the ``/ui/`` URL path.
The UI can be used to learn how to issue API calls against Scylla Manager Server.
It also provides ``scylla-manager.json`` API spec file.
Using the spec file you can generate Scylla Manager clients in various programming languages.

.. image:: images/swagger.png

Access the UI in your browser
================================

The following procedure instructs how to access a remote Scylla Manager Server UI from your workstation.

**Prerequisites**

#. SSH access to a host running Scylla Manager Server.

**Procedure**

#. Get to know address and port Scylla Manager Server is listening on.
   By default it's localhost and port 5080.
   If that is not the case check the ``http`` and ``https`` configuration options in the :doc:`config file <../config/scylla-manager-config>`.

#. Open an SSH tunnel from your workstation to the Scylla Manager Server host.
   The following example works with the default settings. If needed adjust address and port.

   .. code-block:: none

      ssh -L 5080:localhost:5080 <host>

#. Open `<http://localhost:5080/ui/>`_ in your browser.
