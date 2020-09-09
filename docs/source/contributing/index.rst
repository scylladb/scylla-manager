Contributing
============

Configuring a Python environment
--------------------------------

This project's Python side is managed with `Poetry <https://python-poetry.org/docs/>`_.
It combines several things in one tool:

*   Keeping track of Python dependencies, including ones required only for development and tests.
*   Initializing and managing a virtual environment.
*   Packaging the theme into a package for PyPI (Python Package Index).
    Such package can be installed with ``pip install`` and other tools.

To initialize a Python development environment for this project:

#.  Make sure you have Python version 3.7 or later installed.
#.  `Install Poetry <https://python-poetry.org/docs/>`_.

#.   Initialize a virtual environment with:

    ..  code-block:: bash

        poetry install

    This will create an environment outside of the project directory, somewhere under ``~/Library/Caches/pypoetry/virtualenvs/``.
    You'll see the path in installation logs.

#.  Activate the virtual environment for current shell session:

    ..  code-block:: bash

        poetry shell


    Alternatively, if you use PyCharm, you can configure the project to use this environment as the default Python interpreter.
    PyCharm doesn't detect Poetry (yet), so just use the "virtualenv" option and
    provide the full path to the environment.
