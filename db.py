import datetime
import operator
import os
from typing import List, Dict, Any

from db_api import DBField, SelectionCriteria
from json_func import write_to_json, add_table_json, read_from_json, add_to_json

ops = {">": operator.gt, "<": operator.lt, "=": operator.eq, ">=": operator.ge, "<=": operator.le, "!=": operator.ne, }
types = {"str": str, "int": int, "datetime": datetime}


def read_db_meta_data():
    return read_from_json("db_files/db.json")


def update_db_meta_data(db_meta_data):
    write_to_json("db_files/db.json", db_meta_data)


def file_exists(path):
    return os.path.isfile(path)

def del_if_record_appear(path, key):
    file_data = read_from_json(path)
    record_exist =False
    for k in file_data.keys():
        if k == str(key):
            record_exist = True
            del_key = k
            break

    if record_exist:
        del file_data[del_key]
        write_to_json(path,file_data)

    return record_exist

def insert_new_record(values, primary_key, name, db_meta_data):

    new_record = {values[primary_key]: {k: str(v) for k, v in values.items() if k != primary_key}}

    if db_meta_data[name]['num_of_lines'] % 10 == 0 and db_meta_data[name]['num_of_lines'] != 0:

        if file_exists(f"db_files/{name}{db_meta_data[name]['num_of_files'] + 1}.json"):
            raise ValueError

        db_meta_data[name]['num_of_files'] += 1
        write_to_json(f"db_files/{name}{db_meta_data[name]['num_of_files']}.json", new_record)

    else:
        add_to_json(f"db_files/{name}{db_meta_data[name]['num_of_files']}.json", new_record)

    db_meta_data[name]['num_of_lines'] += 1
    update_db_meta_data(db_meta_data)


def delete_table_from_db(table_name):
    db_meta_data = read_db_meta_data()
    num_of_files = db_meta_data[table_name]["num_of_files"]
    del db_meta_data[table_name]
    db_meta_data['num_of_tables'] -= 1
    update_db_meta_data(db_meta_data)
    return num_of_files


def convert_from_DBFields(fields):
    fields_names = {}
    for field in fields:
        fields_names[field.name] = field.type.__name__
    return fields_names


def convert_to_DBFields(fields):
    db_fields_list = []
    for k, v in fields.items():
        db_fields_list.append(DBField(k, types[v]))
    return db_fields_list

def meets_all_the_criteria(file_data,key,value,primary_key,criteria):
    flag = True
    for c in criteria:
        if c.field_name == primary_key:
            if key in file_data.keys():
                if not ops[c.operator](int(key), int(c.value)):
                    flag = False
                    break
            elif key in file_data.keys():
                if not ops[c.operator](value[c.field_name], c.value):
                    flag = False
                    break
    return flag


# return_flag = True
# for c in criteria:
#     if c.field_name == primary_key:
#         if not ops[c.operator](int(key), int(c.value)):
#             return_flag = False
#             break
#
#     elif not ops[c.operator](value[c.field_name], c.value):
#         return_flag = False
#         break
class DBTable:
    def __init__(self, name: str, fields: List[DBField], key_field_name: str):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

    def count(self) -> int:
        return read_db_meta_data()[self.name]["num_of_lines"]

    def insert_record(self, values: Dict[str, Any]) -> None:

        record_exists_flag = None
        db_meta_data = read_db_meta_data()
        primary_key = db_meta_data[self.name]['key_field_name']

        try:
            record_exists_flag = self.get_record(values[primary_key])

        except ValueError:

            # if record does not exist
            insert_new_record(values, primary_key, self.name, db_meta_data)

        if record_exists_flag is not None:
            raise ValueError

    def delete_record(self, key: Any) -> None:

        db_meta_data = read_db_meta_data()
        num_of_files = db_meta_data[self.name]['num_of_files']
        record_exists = False
        for file_num in range(num_of_files):

            if del_if_record_appear(f"db_files/{self.name}{file_num + 1}.json",key) is True:
                record_exists = True
                break

        if record_exists is False:
            raise ValueError

        db_meta_data[self.name]['num_of_lines'] -= 1
        update_db_meta_data(db_meta_data)

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        db_meta_data = read_db_meta_data()
        num_of_files = db_meta_data[self.name]['num_of_files']
        primary_key = db_meta_data[self.name]['key_field_name']

        for file_num in range(num_of_files):
            keys_to_delete = []
            file_data = read_from_json(f"db_files/{self.name}{file_num + 1}.json")

            for key, value in file_data.items():

                if meets_all_the_criteria(file_data,key,value,primary_key,criteria):
                    keys_to_delete.append(key)

            for key in keys_to_delete:
                del file_data[key]
                db_meta_data[self.name]['num_of_lines'] -= 1

            write_to_json(f"db_files/{self.name}{file_num + 1}.json", file_data)
        update_db_meta_data(db_meta_data)

    def get_record(self, key: Any) -> Dict[str, Any]:
        db_meta_data = read_db_meta_data()
        num_of_files = db_meta_data[self.name]['num_of_files']

        for file_num in range(num_of_files):
            file_data = read_from_json(f"db_files/{self.name}{file_num + 1}.json")

            for k in file_data.keys():
                if k == str(key):
                    return file_data[k]

        raise ValueError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        db_meta_data = read_db_meta_data()
        num_of_files = db_meta_data[self.name]['num_of_files']
        for file_num in range(num_of_files):
            file_data = read_from_json(f"db_files/{self.name}{file_num + 1}.json")
            for k in file_data.keys():
                if k == str(key):
                    for field, val in values.items():
                        file_data[k][field] = val
                    write_to_json(f"db_files/{self.name}{file_num + 1}.json", file_data)
                    return None

        raise ValueError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        db_meta_data = read_db_meta_data()
        num_of_files = db_meta_data[self.name]['num_of_files']
        primary_key = db_meta_data[self.name]['key_field_name']
        records_to_return = []
        for file_num in range(num_of_files):
            file_data = read_from_json(f"db_files/{self.name}{file_num + 1}.json")
            for key, value in file_data.items():
                for c in criteria:
                    if c.field_name == primary_key:
                        if not ops[c.operator](int(key), int(c.value)):
                            break

                    elif not ops[c.operator](value[c.field_name], c.value):
                        break
                    records_to_return.append(value)
        return records_to_return

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


class DataBase:
    def __init__(self):
        if not file_exists("db_files/db.json"):
            write_to_json("db_files/db.json", {"num_of_tables": 0})

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        table_fields = [field.name for field in fields]

        if key_field_name not in table_fields:
            raise ValueError
        if file_exists(f"db_files/{table_name}1.json"):
            raise ValueError

        table_data = {"fields": convert_from_DBFields(fields), "key_field_name": key_field_name, "num_of_files": 1, "num_of_lines": 0}
        add_table_json(table_data, table_name)
        return DBTable(table_name, fields, key_field_name)

    def num_tables(self) -> int:
        return read_db_meta_data()['num_of_tables']

    def get_table(self, table_name: str) -> DBTable:
        db_meta_data = read_db_meta_data()
        return DBTable(table_name, convert_to_DBFields(db_meta_data[table_name]['fields']), db_meta_data[table_name]['key_field_name'])

    def delete_table(self, table_name: str) -> None:
        num_of_files_to_delete = delete_table_from_db(table_name)

        for num in range(num_of_files_to_delete):
            if file_exists(f'db_files/{table_name}{num + 1}.json'):
                os.remove(f"db_files/{table_name}{num + 1}.json")

    def get_tables_names(self) -> List[Any]:
        db_meta_data = read_db_meta_data()
        return [key for key in db_meta_data.keys() if key != 'num_of_tables']

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
