import os
import socket
import warnings

import sqlalchemy


def check_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(("host.docker.internal", 9191))
    if result != 0:
        warnings.warn("host.docker.internal:9191 is not open; did you start the Kinetica Docker?")
    sock.close()


def check_so_file():
    if not os.path.exists("/etc/libKineticaODBC.so"):
        raise FileNotFoundError(
            "You need to add libKineticaODBC.so at /etc/ on the same server as the SQLAlchemy connection."
            "To find the file, see here: https://docs.kinetica.com/7.0/connectors/sql_guide.html#installing-unixodbc"
        )


def pytest_sessionstart():
    check_port()
    check_so_file()
    sqlalchemy.dialects.registry.register("sa_gpudb", "sa_gpudb.pyodbc", "dialect")
    sqlalchemy.dialects.registry.load("sa_gpudb")
