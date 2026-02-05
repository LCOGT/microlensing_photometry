===============
Troubleshooting
===============

Here are a symptoms a some known issues and how to fix them.

```Failed to reach API```
-------------------------

**Symptom:**
When one of the pipelines is run, it crashes with the following error
message:

.. code-block::

        raise RuntimeError(f"Failed to reach API at {self.api_url}") from e
        RuntimeError: Failed to reach API at http://127.0.0.1:4200/api/

**Diagnosis:**
The pipeline is trying to send status output to the Prefect server, but
the server isn't running.

**Solution:**
Start the prefect server:

.. code-block::

    venv> prefect server start
