sqlalchemy-gpudb
================


Prerequisites
-----

For Centos/Redhat:

```sudo dnf install unixODBC-devel gcc-c++ python-devel
``` 

For Ubuntu/Debian:

```sudo apt-get install cpp unixodbc-dev python-dev
``` 

Usage
-----

0. Copy the Kinetica .so to the /etc/ file of the server where the SQLAlchemy engine will execute (e.g. SuperSet's execution environment)
1. `pip install git+https://github.com/ajduberstein/sqlalchemy_gpudb`
2. Register the dialect and run SQL, as you can see in `test_program.py` in this directory.

TODO
-----

Assumptions baked in here so far:

- Automate Docker-based tests
- You're installing this in a Debian-based Docker container–need to make this more flexible
- You're running Kinetica in a Docker container on the same server (this will need to change)
- You've run `apt-get update && apt-get install unixodbc-dev` on the Docker container
- Need to connect to a prod Kinetica instance securely

Errors and solutions
--------------------

- `missing sql.h`: Need to install `unixodbc-dev`
- `[unixODBC][Driver Manager]Data source name not found, and no default driver specified`: No named connection in the .ini file
- `GPUdb unavailable (no backup cluster exists`: Need to click "Start" on Kinetica GUI
- A message saying there's no function called `schema_name()`: Need to register the SQLAlchemy dialect correctly:

```python
sqlalchemy.dialects.registry.register('sa_gpudb', 'sa_gpudb.pyodbc', 'dialect')
sqlalchemy.dialects.registry.load('sa_gpudb')
```

To create a connector, the following works well:

```
sqlalchemy.create_engine(
    'sa_gpudb://KINETICA',
    connect_args = {'autocommit': True, 'fast_executemany': False},
).connect()
```


Development process
--------------------

I've been building serving this using [pypi-server](https://pypi.org/project/pypiserver/) and installing within the Python docker container:

```bash
pip install -i http://host.docker.internal:1234/simple/ --trusted-host host.docker.internal sqlalchemy sqlalchemy-gpudb
```

I run the code at `test_program.py` to verify that the connection works;

Additional connections need to be added and named in `/etc/odbc.ini`, and auth must be provided there.
