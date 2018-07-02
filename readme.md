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
