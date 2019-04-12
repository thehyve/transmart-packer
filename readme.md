# transmart-packer

Run data transformation jobs for transmart.

## install

First make virtual environment to install dependencies using `Python3.6+`

```bash
pip install -r requirements
```

## dependencies

* a redis server running on localhost (or update `packer/config.py`)

## running 

From root dir run:

```bash
redis-server
``` 

```bash
celery -A packer.tasks worker --loglevel=info
``` 

and

```bash
python -m packer
``` 

Alternatively, you could build and run the stack from code using docker-compose. This
has only been tested using Docker for Mac, but should work regardless.

```bash
# Downloads redis image and creates image with project dependencies.
docker-compose build

# After build is complete, start via:
docker-compose up
``` 

On code change the webserver will automatically restart, but the Celery workers will not.
After updating the Celery task logic, you will need to restart the Docker container.

## Testing
To run the test suite, we have to start redis-server and celery workers with the commands above.
Then you can run:

```bash
python -m unittest discover -s ./tests
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


## Existing jobs

### Basic export job

Export transmart api client observation dataframe to tsv file

```json
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
```

### Patient, diagnosis, biosource and biomaterial export

Exports patient > diagnosis > biosource > biomaterial hierarchy as first 4 columns of the file.
The rest of the columns are concepts. Higher level concepts (e.g Age that is specific to Patient level)
get distributed to all rows specific to lower levels (e.g. Diagnosis)

See [the CSR](https://github.com/thehyve/transmart-core/tree/dev/transmart-data/test_studies/CSR) test study as an example.

```json
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
```

where:

 - `job_parameters.constraint` - any [transmart v2 api constraint](https://github.com/thehyve/transmart-core/blob/dev/open-api/swagger.yaml)
or composition of them that used to get data from transmart.

 - `job_parameters.custom_name`(optional) - name of the export job and the output `tsv` file.
 - `job_parameters.row_filter` (optional) - any [transmart v2 api constraint](https://github.com/thehyve/transmart-core/blob/dev/open-api/swagger.yaml)
 or composition of them to fetch keys ([[[[patient], diagnosis], biosource], biomaterial]) that will make it to the end result.
 e.g. Given the `CSR` study and query above only rows specific to `P2` and `P6` patients will end up to the result table such as `P2`, `D2`, `BS2`, `BM2`, ... row.
 Please note that keys do not have to be equals in length. A row gets selected if only part of keys matches. e.g. `P1` vs `P1`, `D1`
