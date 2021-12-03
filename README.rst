Documentation
=============

|Documentation Status| |PyPI Status| |CI Test| |Coverage|

Running tests
-------------

To run pytest in currently installed environment:

.. code:: bash

   poetry run pytest

To run full extensive test suite:

.. code:: bash

   poetry run invoke test

Formatting and linting
----------------------

Formatting and linting is done with a single command. First formats,
then lints.

.. code:: bash

   poetry run invoke format-and-lint

Building docs
-------------

Docs can be built locally to test that ``ReadTheDocs`` can also build them:

.. code:: bash

   poetry run invoke docs

Invoke usage
------------

To list all available commands from ``tasks.py``:

.. code:: bash

   poetry run invoke --list

Development
~~~~~~~~~~~

Development dependencies include:

   -  invoke
   -  nox
   -  copier
   -  pytest
   -  coverage
   -  sphinx

Big thanks to all maintainers of the above packages!

License
~~~~~~~

Copyright © 2021, Nikolas Ovaskainen.

-----


.. |Documentation Status| image:: https://readthedocs.org/projects/tracerepo/badge/?version=latest
   :target: https://tracerepo.readthedocs.io/en/latest/?badge=latest
.. |PyPI Status| image:: https://img.shields.io/pypi/v/tracerepo.svg
   :target: https://pypi.python.org/pypi/tracerepo
.. |CI Test| image:: https://github.com/nialov/tracerepo/workflows/test-and-publish/badge.svg
   :target: https://github.com/nialov/tracerepo/actions/workflows/test-and-publish.yaml?query=branch%3Amaster
.. |Coverage| image:: https://raw.githubusercontent.com/nialov/tracerepo/master/docs_src/imgs/coverage.svg
   :target: https://github.com/nialov/tracerepo/blob/master/docs_src/imgs/coverage.svg
