# transmart-packer

Run data transformation jobs for transmart.

## install

First make virtual environment to install dependencies.

```bash
pip install -r requirements
```

## dependencies

* a redis server running on localhost (or update `packer/config.py`)

## running 

From root dir run:

```bash
celery -A packer.tasks worker --loglevel=info
``` 

and

```bash
python -m packer.main
``` 



## Usage

Available handlers:
- `/jobs`
   - List all known jobs for this user.
- `/jobs/create`
   - Create a new job by providing `job_type` and `job_parameters`, creates the job and returns a `task_id`.
- `/jobs/status/<task_id>`
   - Get status details for a specific task.
- `/jobs/cancel/<task_id>`
   - Cancel scheduled or abort a running task.
- `/jobs/data/<task_id>`
   - Download the data that this task produced.
- `/jobs/subscribe`
   - Open websocket connection to get live updates on job progress. 

To start the toy job "add" on the localhost machine 
make call to `http://localhost:8999/jobs/create?job_type=add&job_parameters={%22x%22:500,%22y%22:1501}`.


## Extending
New jobs can be added by adding a new Celery function to the jobs folder and adding 
the function to the jobs registry. See the ./packer/jobs/example.py to learn how.