
from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

# Make an exception
class CustomerException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
            "columns": None
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:

                cols = self._data.get("columns", None)
                if cols is None:
                    cols = r.keys()
                    self._data["columns"] = cols

                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

    def get_key_columns(self):
        key_cols = self._data.get("key_columns")
        return key_cols

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        key_cols = self.get_key_columns()
        tmp = dict(zip(key_cols, key_fields))
        result = self.find_by_template(template=tmp, field_list=field_list)

        if result is not None and len(result) > 0:
            result = result[0]
        else:
            result = None

        return result

    # helper method for find_by_template
    def find_tmp_row(row, field_list):
        result = {}

        if field_list is None:
            return row

        for f in field_list:
            result[f] = row[f]

        return result

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        # pass
        result = []
        for r in self._rows:
            if CSVDataTable.matches_template(r, template):
                new_r = CSVDataTable.find_tmp_row(r, field_list)
                result.append(new_r)

        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :return: A count of the rows deleted.
        """

        res = 0

        r = self.find_by_primary_key(key_fields=key_fields)

        if r is not None:
            res += 1
            self._rows.remove(r)

        return res

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        result = 0
        for r in self._rows:
            if CSVDataTable.matches_template(r, template):
                self._rows.remove(r)
                result += 1

        return result

    def update_by_key(self, key_fields, new_values):
        """
        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        res = 0
        r = self.find_by_primary_key(key_fields=key_fields)
        if r is not None:
            r.update(new_values)
            res += 1

        return res

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        result = 0
        for r in self._rows:
            if CSVDataTable.matches_template(r, template):
                # Make sure that new_values are valid;
                r.update(new_values)
                result += 1

        return result

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        if new_record is None:
            raise ValueError("New record is None.")

        new_cols = set(new_record.keys())
        tbl_cols = set(self._data["columns"])

        if not new_cols.issubset(tbl_cols):
            raise ValueError("New records are not part of the fields")

        key_cols = self._data.get("key_columns", None)

        if key_cols is not None:
            key_cols = set(key_cols)
            if not key_cols.issubset(new_cols):
                raise ValueError("Key columns are not included on new records.")

        for k in key_cols:
            if new_record.get(k, None) is None:
                raise ValueError("The field of "+k+" in new record is none.")

    #         key_tmp = self.get_key_template(new_record)
    #         if self.find_by_template(key_tmp) is not None
    #             and len(self.find_by_template(key_cols)) > 0:
    #             raise ValueError("...")
    #
    #     self._rows.append(new_record)


        try:
            if(self.find_by_template(template=new_record)):
                raise CustomerExceotion("Duplicated record:")
            else:
                self._add_row(r=new_record)
        except CustomerExceotion:
            print("You have inserted a duplicated record.")

    def get_rows(self):
        return self._rows

