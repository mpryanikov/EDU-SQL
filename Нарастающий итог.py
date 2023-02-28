# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

import pandas as pd
import numpy as np

# ### Подключение к бд

import sqlalchemy
# sqlalchemy.__version__

# # !pip install pyodbc
import pyodbc
import warnings
warnings.filterwarnings('ignore')

conn = pyodbc.connect('DSN=TestDB;Trusted_Connection=yes;')


def select(sql):
  return pd.read_sql(sql,conn)


# # Нарастающий итог

# ## Задание 'Совкомбанк_технологии' - 220901

cur = conn.cursor()
sql = """
drop table if exists tbl;

CREATE TABLE tbl(
    Id    Int,
    Value Int 
)    
insert into tbl values
    (1, 10),
    (2, -5),
    (3, 3),
    (4, 8),
    (5, -2)
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from tbl'
select(sql)

sql = """
select t.*,
    sum(t.Value) over(order by t.id) as Summ
from tbl t
"""
select(sql)

sql = """
select t.Id, t.Value,
    sum(t1.Value) as Summ
from tbl t
left join tbl t1 on t1.id <= t.id
group by t.Id, t.Value
order by t.Id, t.Value
"""
select(sql)


