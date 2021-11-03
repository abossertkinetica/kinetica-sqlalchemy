# See README.md
import sa_gpudb

import sqlalchemy
from sqlalchemy import create_engine


def test_execution():
    engine = create_engine("sa_gpudb://KINETICA", connect_args={"autocommit": True, "fast_executemany": False})
    result = engine.execute("select 1")
    assert [row for row in result][0][0] == 1
