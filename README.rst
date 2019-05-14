transmart-packer
================

|Build status| |codecov| |pypi| |downloads|

.. |Build status| image:: https://travis-ci.org/thehyve/transmart-packer.svg?branch=master
   :alt: Build status
   :target: https://travis-ci.org/thehyve/transmart-packer/branches
.. |codecov| image:: https://codecov.io/gh/thehyve/transmart-packer/branch/master/graph/badge.svg
   :alt: codecov
   :target: https://codecov.io/gh/thehyve/transmart-packer
.. |pypi| image:: https://img.shields.io/pypi/v/transmart-packer.svg
   :alt: PyPI
   :target: https://pypi.org/project/transmart-packer/
.. |downloads| image:: https://img.shields.io/pypi/dm/transmart-packer.svg
   :alt: PyPI - Downloads
   :target: https://pypi.org/project/transmart-packer/

Run data transformation jobs for TranSMART_.

.. _TranSMART: https://github.com/thehyve/transmart-core


Install
-------

First make virtual environment to install dependencies using `Python3.6+`

.. code-block:: bash

    pip install transmart-packer

Or from source:

.. code-block:: bash

  git clone https://github.com/thehyve/transmart-packer.git
  cd transmart-packer
  pip install .


Dependencies
------------

* a Redis server running on localhost (or update ``packer/config.py``)


Running
-------

From root dir run:

.. code-block:: bash

  redis-server

  celery -A packer.tasks worker --loglevel=info

  transmart-packer


Alternatively, you could build and run the stack from code using ``docker-compose``. This
has only been tested using Docker for Mac, but should work regardless.

.. code-block:: bash

    # Downloads redis image and creates image with project dependencies.
    docker-compose build

    # After build is complete, start via:
    docker-compose up

On code change the webserver will automatically restart, but the Celery workers will not.
After updating the Celery task logic, you will need to restart the Docker container.


Usage
-----

Available handlers:

==============================  =================
Path                            Description
==============================  =================
``GET /jobs``                   List all known jobs for this user.
``POST /jobs/create``           Create a new job by providing `job_type` and `job_parameters`, creates the job and returns a `task_id`.
``GET /jobs/status/<task_id>``  Get status details for a specific task.
``GET /jobs/cancel/<task_id>``  Cancel scheduled or abort a running task.
``GET /jobs/data/<task_id>``    Download the data that this task produced.
``WS /jobs/subscribe``          Open websocket connection to get live updates on job progress.
==============================  =================

To start the toy job "add" on the localhost machine
make call to ``http://localhost:8999/jobs/create?job_type=add&job_parameters={%22x%22:500,%22y%22:1501}``.


Development
-----------

Testing
^^^^^^^

To run the test suite, we have to start redis-server and celery workers with the commands above.
Then you can run:

.. code-block:: bash

    python setup.py test

Extending
^^^^^^^^^

New jobs can be added by adding a new Celery function to the jobs folder and adding
the function to the jobs registry. See the `packer/jobs/example.py`_ to learn how.

.. _packer/jobs/example.py: packer/jobs/example.py


Existing jobs
-------------

Basic export job
^^^^^^^^^^^^^^^^

Export transmart api client observation dataframe to tsv file

.. code-block:: json

    {
        "job_type":"basic_export",
        "job_parameters": {
            "constraint": {
                "type":"study_name",
                "studyId":"CSR"

            },
            "custom_name":"name of the export"
        }
    }


Patient, diagnosis, biosource and biomaterial export
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Exports patient > diagnosis > biosource > biomaterial hierarchy as first 4 columns of the file.
The rest of the columns are concepts. Higher level concepts (e.g Age that is specific to Patient level)
get distributed to all rows specific to lower levels (e.g. Diagnosis)

See the CSR_ test study as an example.

.. _CSR: https://github.com/thehyve/transmart-core/tree/dev/transmart-data/test_studies/CSR

.. code-block:: json

    {
        "job_type":"patient_diagnosis_biosource_biomaterial_export",
        "job_parameters": {
            "constraint": {
                "type":"study_name",
                "studyId":"CSR"

            },
            "custom_name":"name of the export",
            "row_filter": {
                "type":"patient_set",
                "subjectIds": ["P2", "P6"]
            }
        }
    }


where:

- ``job_parameters.constraint`` - any `transmart v2 api constraint`_
  or composition of them that used to get data from transmart.
- ``job_parameters.custom_name`` (optional) - name of the export job and the output ``tsv`` file.
- ``job_parameters.row_filter`` (optional) - any `transmart v2 api constraint`_
  or composition of them to fetch keys (``[[[[patient], diagnosis], biosource], biomaterial]``) that will make it to the end result.
  e.g. Given the `CSR` study and query above only rows specific to `P2` and `P6` patients will end up to the result table such as `P2`, `D2`, `BS2`, `BM2`, ... row.
  Please note that keys do not have to be equals in length. A row gets selected if only part of keys matches. e.g. `P1` vs `P1`, `D1`

.. _`transmart v2 api constraint`: https://github.com/thehyve/transmart-core/blob/dev/open-api/swagger.yaml

License
-------

Copyright © 2019 The Hyve B.V.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the `GNU Affero General Public License`_
along with this program. If not, see https://www.gnu.org/licenses/.

.. _`GNU Affero General Public License`: LICENSE
