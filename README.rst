Documentation
=============

|Documentation Status| |PyPI Status| |CI Test| |Coverage|

Running tests
-------------

To run pytest in currently installed environment:

.. code:: bash

   poetry run pytest

Development
~~~~~~~~~~~

Development dependencies for ``tracerepo`` include:

-  ``poetry``

   -  Used to handle Python package dependencies.

   .. code:: bash

      # Use poetry run to execute poetry installed cli tools such as invoke,
      # nox and pytest
      poetry run

-  ``pytest``

   -  ``pytest`` is a Python test runner. It is used to run defined tests to
      check that the package executes as expected. The defined tests in
      ``./tests`` contain many regression tests (done with
      ``pytest-regressions``) that make it almost impossible
      to add features to ``tracerepo`` that changes the results of functions
      and methods.

-  ``coverage``

   -  To check coverage of tests

-  ``sphinx``

   -  Creates documentation from files in ``./docs_src``.

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
