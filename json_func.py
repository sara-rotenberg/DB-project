import json
from os import close


def write_to_json(path,data):
    with open(path, "w") as the_file:
        json.dump(data, the_file)



def add_to_json(path,data):
    the_file = open(path)
    json_data = json.load(the_file)
    with open(path, "w") as the_file:
        json_data.update(data)
        json.dump(json_data, the_file)


def read_from_json(path):
    the_file = open(path)
    data = json.load(the_file)
    return data

def add_table_json(data,table_name):
    json_data = read_from_json("db_files/db.json")
    with open("db_files/db.json", "w") as the_file:
        json_data[table_name] = data
        json_data["num_of_tables"] +=1
        json.dump(json_data, the_file)
        write_to_json(f"db_files/{table_name}1.json", {})

