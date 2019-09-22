# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.RDBDataTable import RDBDataTable
import logging
import os
import json



# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")

def t_find_by_pk():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'stint', 'AB', 'H', 'HR', 'RBI']
    tmp = {'teamID': 'BOS', 'yearID': '1960'}
    key_vals = ['willite01', 'BOS', '1960', '1']

    rdb_tbl = RDBDataTable("batting", connect_info, key_cols)

    res = rdb_tbl.find_by_primary_key(key_fields=key_vals)

    # print("Created table = " + str(csv_tbl))
    print("Query result = \n", json.dumps(res, indent=2))

def t_find_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'stint', 'AB', 'H', 'HR', 'RBI']
    tmp = {'teamID': 'BOS', 'yearID': '1960'}

    rdb_tbl = RDBDataTable("batting", connect_info, key_cols)

    res = rdb_tbl.find_by_template(template=tmp, field_list=fields)

    # print("Created table = " + str(csv_tbl))
    print("Query result = \n", json.dumps(res, indent=2))

def t_delete_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'stint', 'AB', 'H', 'HR', 'RBI']
    tmp = {'teamID': 'BOS', 'yearID': '1960'}
    key_vals = ['barkeal01', 'RC1', '1871', '1']

    rdb_tbl = RDBDataTable("batting", connect_info, key_cols)

    res = rdb_tbl.delete_by_key(key_fields=key_vals)

    # print("Created table = " + str(csv_tbl))
    print("Query result = ", res)

def t_delete_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'stint', 'AB', 'H', 'HR', 'RBI']
    tmp = {'playerID':'barnero01', 'teamID':'BS1', 'yearID':'1871', 'stint':'1'}

    csv_tbl = RDBDataTable("batting", connect_info, key_cols)

    res = csv_tbl.delete_by_template(template=tmp)

    # print("Created table = " + str(csv_tbl))
    print("Query result = ", res)

def t_update_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'stint', 'AB', 'H', 'HR', 'RBI']
    tmp = {'teamID': 'BOS', 'yearID': '1960'}
    key_vals = ['allisar01', 'CL1', '1871', '1']

    csv_tbl = RDBDataTable("batting", connect_info, key_cols)

    res = csv_tbl.update_by_key(key_fields=key_vals, new_values={
        "playerID": "allisar01",
        "teamID": "CL1",
        "yearID": "1871",
        "stint": "2",
        "AB": "1",
        "H": "100",
        "HR": "100",
        "RBI": "100"
    })

    # print("Created table = " + str(csv_tbl))
    print("Query result = ", res)

def t_update_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'stint', 'AB', 'H', 'HR', 'RBI']
    tmp = {"playerID": "allisar01",
        "teamID": "CL1",
        "yearID": "1871",
        "stint": "2"}

    csv_tbl = RDBDataTable("batting", connect_info, key_cols)

    res = csv_tbl.update_by_template(template=tmp, new_values={
        "playerID": "allisar01",
        "teamID": "CL1",
        "yearID": "1871",
        "stint": "3",
        "AB": "1",
        "H": "100",
        "HR": "100",
        "RBI": "100"
      })

    # print("Created table = " + str(csv_tbl))
    print("Query result = ", res)

def t_insert():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'stint', 'AB', 'H', 'HR', 'RBI']
    tmp = {'teamID': 'BOS', 'yearID': '1960'}
    new_r = {
        "playerID": "a",
        "teamID": "BOS",
        "yearID": "1960",
        "stint": "1",
        "AB": "1",
        "H": "100",
        "HR": "100",
        "RBI": "100"
    }

    csv_tbl = RDBDataTable("batting", connect_info, key_cols)

    res = csv_tbl.insert(new_record=new_r)

    print("Created table = " + str(csv_tbl))
    # print("Query result = \n", json.dumps(res, indent=2))




# t_find_by_pk()
# t_find_by_template()
# t_delete_by_key()
t_delete_by_template()
# t_update_by_key()
# t_update_by_template()
# t_insert()