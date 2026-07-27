Environment variables
---------------------

The variables presented in this section can be used with any sctool command.

sctool uses the following environment variables:

* `SCYLLA_MANAGER_CLUSTER` - if set, specifies the default value for the ``-c, --cluster`` flag, in commands that support it.
* `SCYLLA_MANAGER_API_URL` - if set, specifies the default value for the ``--api-url`` flag; it can be useful when using sctool with a remote Scylla Manager server.

The environment variables may be saved in  your ``~/.bashrc`` file so that the variables are set after login.
