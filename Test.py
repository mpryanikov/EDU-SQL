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


# # Задание Diasoft

# ---

# ## Задание 1

# Есть такая структура данных из двух таблиц.  
# Таблица 1.  
# Department – отделы  
# Поля:  
# ID – Идентификатор отдела  
# Name – Название отдела  
#   
# Таблица 2.  
# Employee – сотрудники  
# Поля:  
# ID – Идентификатор сотрудника  
# Department_ID – идентификатор отдела  
# Chief_ID – идентификатор непосредственного руководителя  
# Name – Имя сотрудника  
# SALARY – Заработная плата сотрудника  

cur = conn.cursor()
sql = """
drop table if exists Department;

CREATE TABLE Department(
    Id    Int,
    Name  VarChar(max) 
) 

Insert into Department values
    (1, 'Department_1'),
    (2, 'Department_2'),
    (3, 'Department_3')
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from Department'
select(sql)

cur = conn.cursor()
sql = """
drop table if exists Employee;

CREATE TABLE Employee(
    Id            Int,
    Department_ID Int,
    Chief_ID      Int,
    Name          VarChar(max),
    SALARY        Money
) 

Insert into Employee values
    (1, 1, Null, 'ARTEM',    25000.0),
    (2, 1, 1,    'IVAN',     27000.0),
    (3, 2, 1,    'PETR',     23000.0),
    (4, 2, 3,    'DMITRY',   22000.0),
    (5, 2, 4,    'VASILY',   28000.0),
    (6, 2, Null, 'AFANASIY', 33000.0),
    (7, 3, Null, 'IGOR',     13000.0),
    (8, 3, 7,    'KIRILL',   15000.0)
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from Employee'
select(sql)

# #### Требуется составить SQL-запросы, для решения следующих заданий:

# <a href='https://github.com/F1garr0/sqlTasks/blob/master/Untitled20.md'>
# Нашел в интернете на GitHub</a>

# ### Задание 1

# #### Вывести список сотрудников, получающих заработную плату большую чем у непосредственного руководителя

sql = """
select EMP1.Name 
from Employee EMP1, Employee EMP2 
WHERE EMP1.Chief_ID = EMP2.Id and EMP1.SALARY > EMP2.SALARY
"""
select(sql)

# ### Задание 2

# #### Вывести список сотрудников, получающих максимальную заработную плату в своем отделе

sql = """
select Employee.Name, ms.max_salary
from Employee, (
                    select Department_ID, max(SALARY) as max_salary 
                    from Employee 
                    group by Department_ID
                ) ms
where Employee.Department_ID = ms.Department_ID and Employee.SALARY = ms.max_salary
"""
select(sql)

# ### Задание 3

# #### Вывести список ID отделов, количество сотрудников в которых не превышает 3 человек

sql = """
select Department_ID 
from Employee 
group by Department_ID 
having count(*) <= 3
"""
select(sql)

# ### Задание 4

# #### Вывести список сотрудников, не имеющих назначенного руководителя, работающего в том-же отделе

sql = """
select A.Name
from Employee A, Employee B
where A.Chief_ID is null
or
    (
    A.Chief_ID = B.Id
    and
    A.Department_ID ! = B.Department_ID
    )
group by A.Name
"""
select(sql)

# ### Задание 5

# #### Найти список ID отделов с максимальной суммарной зарплатой сотрудников

sql = """
with 
max_salary (Department_ID, sum_s) as(
    select Department_ID, sum(SALARY) as sum_s 
    from Employee 
    group by Department_ID 
    order by sum_s desc 
    OFFSET 0 ROWS FETCH FIRST 1 ROWS ONLY
),
deps_summary (Department_ID, summ) as(
    select Department_ID, sum(SALARY) as summ 
    from Employee 
    group by Department_ID
)
select deps_summary.Department_ID 
from deps_summary 
where deps_summary.summ = (select sum_s from max_salary)
"""
select(sql)

# ---

# ## Задание 2

# ### Задание 1

# + active=""
# Есть таблица документов tDealTransact с полями Date - дата платежа, InstSecondID - идентификатор банка-получателя платежа; таблица банков tInstitution с полями InstitutionID - идентификатор банка, Name - наименование банка.
# Необходимо 
# a) Получить список банков, получивших платежи
# в период с 1 января 2001 года по 5 апреля 2001 года.
# б) Этот список должен быть отсортирован по наименованию
# и один и тот же банк должен быть представлен один раз.
# в) Список должен содержать количество документов, отправленных
# каждому банку.
#
# Запрос должен удовлетворять всем трем условиям.
# -







# ### Задание 2

# + active=""
# Есть таблица документов tDealTransact с полями:
# ResourceID numeric(15, 0) - счет по дебету
# ResourcePsvID numeric(15, 0) - cчет по кредиту
# Direction int - направление платежа
# 0 - от нас (те кор. счет стоит по кредиту )
# 1 - на нас (те кор. счет стоит по дебету)
#
# Надо выдать все кор.счета одним select
# 1) с использованием Union
# 2) без использования Union
# -







# ### Задание 3

# + active=""
# Есть таблица документов tDealTransact с полем DocNumber - номер документа - varchar(20) (для разных категорий документов там могут хранится не только цифры например 123 0123 123/23 A1/S2 и т.д.)
# Есть таблица пришедших из РКЦ документов cRmaket с полем Number - int в котором содержаться 3 последние цифры номера.
# Необходимо документы сквитовать по номерам (3 последние цифры 
# DocNumber = Number) одним select-ом, т.е.
#
# DocNumber Number
# '123' = 123
# '0123' = 123
# '10123' = 123
# '3' = 3
# ' 3' = 3
# '03' = 3
# '1003' = 3
# '103' <> 123
# 'A123' = 123
# 'F3' <> 3
# Вывести tDealTransact.DocNumber
# -







# ### Задание 4

# + active=""
# Необходимо по данным аудита отобрать всех клиентов юридических лиц, добавленных и измененных за период с 1-го марта 2011г. по 31-е марта 2012г. В результате должны быть только уникальные юр. лица
# Стр-ра таблиц:
# tInstitution – таблица клиентов
#                      (InstitutionID – уникальный идентификатор клиентов,
#                       PropDealPart – признак типа клиента(0 – юр. лицо, 1 – физ. лицо),
#                       Name – название\наименование клиента)
# hInstitution – история изменений клиентов
#                        (HistoryHeaderID – ссылка на идентификатор аудита
#                         InstitutionID – идентификатор клиента
#                         Name – история изменений поля название\наименование клиента
#                         INN - история изменений поля ИНН клиента
#                         MainMember - история изменений резидентности клиента)
# tAudit          – аудит системы
#                        (AuditID – идентификатор аудита,
#                         InDateTime – дата изменений,
#                         Action – Выполненное действие, ссылка на справочник свойств системы(поле PropVal таблицы  tProperty)  )
# tProperty        -  справочник свойств системы
#                           (PropType - тип свойства, нас интересует только PropType=76
#                             PropVal – значения св-в для конкретного PropType
#                                              PropVal = 1 – изменение
#                                              PropVal = 2 – добавление
#                                              PropVal = 3 – удаление
#                             Name    - наименование св-ва)
# -







# ---

conn.close()


