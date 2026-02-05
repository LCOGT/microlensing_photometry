=================
Prefect Workflows
=================

This package uses the `Prefect <https://www.prefect.io/prefect/open-source>`_
workflow orchestration framework to control automated tasks and monitor
their progress.

This library provides a highly function web-based user dashboard
to monitor running process, which is displayed by the built-in
webservice.  This means that it is necessary for this webserver to be
running before running the pipeline's main scripts (or ```flows```
in prefect's nomenclature).

The simplest way to run this service in a local installation is
to run the prefect server from the Python environment that you
have installed this package in.  From a command prompt, type:

.. code-block::

    > source /<path>/venv/bin/activate
    venv> prefect server start

The terminal will display logging output from the server, and you
will be able to access the pipeline's dashboard by typing the
following URL into the location bar of your browser: `http://127.0.0.1:4200 <http://127.0.0.1:4200>`_
