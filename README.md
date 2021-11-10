sqlalchemy-gpudb
================


About
-----

The official sql-alchemy connector for Kinetica. This connector is maintained with focus on working with the 1.4.x framework and maintaining compatibility for usage with Apache Superset.  


Notice
-----

This is a project that is under heavy modification and is not considered GA yet. 


Pre-Requisite System Packages
-----

For Centos/Redhat:

```
sudo dnf install unixODBC-devel gcc-c++ python-devel
``` 

For Ubuntu/Debian:

```
sudo apt-get install cpp unixodbc-dev python-dev
``` 

Configuration and Installation
-----

1. Update the odbc.ini file with the Kinetica connection and then copy the driver library and unix-odbc configuration files to to the /etc/ file of the server where the SQLAlchemy engine will execute:
   
   For Centos/Redhat:

```
bunzip2 dbfiles/libKineticaODBC_rhel.so.bz2 
sudo cp dbfiles/odbc.ini /etc/
sudo cp dbfiles/odbcinst.ini /etc/
sudo cp dbfiles/libKineticaODBC_rhel.so /etc/libKineticaODBC.so
``` 

For Ubuntu/Debian:

```
bunzip2 dbfiles/libKineticaODBC_ubuntu.so.bz2 
sudo cp dbfiles/odbc.ini /etc/
sudo cp dbfiles/odbcinst.ini /etc/
sudo cp dbfiles/libKineticaODBC_ubuntu.so /etc/libKineticaODBC.so
``` 

   
2. Install the package: 
```
pip install git+https://github.com/kineticadb/kinetica-sqlalchemy
```



Create a SQL Alchemy Connnection
--------------------------

To create a connector, the following works well:

```
import sa_gpudb

import sqlalchemy
from sqlalchemy import create_engine

sqlalchemy.dialects.registry.register('sa_gpudb', 'sa_gpudb.pyodbc', 'dialect')
sqlalchemy.dialects.registry.load('sa_gpudb')

sqlalchemy.create_engine(
    'sa_gpudb://KINETICA',
    connect_args = {'autocommit': True, 'fast_executemany': False},
).connect()
```


Errors and solutions
--------------------

- `missing sql.h`: Need to install development packages please see pre reqs section 
 
- `[unixODBC][Driver Manager]Data source name not found, and no default driver specified`: No named connection in the .ini file

- `GPUdb unavailable (no backup cluster exists`: Ensure Kinetica is running, check via Kinetica Admin UI or command line:

```
service gpudb status
```

- A message saying there's no function called `schema_name()`: Need to register the SQLAlchemy dialect correctly:

```python
sqlalchemy.dialects.registry.register('sa_gpudb', 'sa_gpudb.pyodbc', 'dialect')
sqlalchemy.dialects.registry.load('sa_gpudb')
```

 

