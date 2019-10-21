

import flask
from flask import request, jsonify, Response
import json
import sys
import pdb

tablet_server = flask.Flask(__name__)
tablet_server.config["DEBUG"] = True
tablet_server.config
'tablet to server name mapping'
tablet_serv_name_mapping = dict()
'the list of tables in this tablet server'
tables_list = list()
'path of persistent storage'
persistent_storage = '/Users/raghavs/14848/starter/persist/'
'dictionary for table. It will have the properties of table like column name and rows'
table_contents = dict()
'memtable'
mem_table = list()
'default size of the memtable. Initial value is 100'
mem_table_size = 100
'mem_table spill counter'
mem_table_spill_counter = 0
"""
dictionary of rows against the counts of values inserted 
"""
row_counter = dict()
__name__ = 'main'

if __name__ == 'main':
    tablet_server.run(host=sys.argv[1], port=sys.argv[2], debug=True)
# tablet_server.run()
"""
    Method that appends a table name to the 
    list of tables for this server
"""


def create_table_self(table_name, table_info):
    path = persistent_storage + table_name + "_meta_data.mdt"
    file_desc = open(path, 'w')
    file_desc.write("Table:" + table_name + "\n")
    file_desc.close()

    write_ahead_log = persistent_storage + table_name + ".wal"
    file_desc = open(write_ahead_log, 'w')
    file_desc.write("Table:" + table_name + "\n")
    file_desc.close()
    tables_list.append(table_name)
    table_contents[table_name] = table_info
    return Response(status=200)


"""
    API to create a table and return appropriate response
    :return 400 status if cannot parse json 
    :return 409 if the table already exists 
    :return 200 if successfully created table 
"""


@tablet_server.route('/api/tables', methods=['POST'])
def create_table():
    # import pdb;
    # pdb.set_trace()
    # create a table with name
    if request.method == 'POST':
        table_info = request.get_json()
        if table_info is None:
            return Response(status=400)
        for server_name in tables_list:
            print(server_name)
            if table_info['name'] == server_name:
                return Response(status=409)
        # print("Hello")
        if table_info['name'] not in tables_list:
            response = create_table_self(table_info['name'], table_info)
            return response
    if request.method == 'DELETE':
        table_delete()


"""
    List all tables that are present on this tablet
    :return 200 and content with the table info 
    : return 200 with empty content if not tables are defined 
"""


@tablet_server.route('/api/tables', methods=['GET'])
def list_tables():
    # var = request.url
    # print(var)
    # tab_name = var.split("/")[-1]
    # print("list_tables = ", tab_name)
    global tables_list
    print("I am in tables")
    response = dict()
    table_names = list()
    output = dict()
    if len(tables_list) == 0:
        response["tables"] = table_names
        output = jsonify(response)
        print(output)
        output.status_code = 200
        return output
    else:
        for table_n in tables_list:
            table_names.append(table_n)
        response["tables"] = table_names
        output = jsonify(response)
        print(output)
        output.status_code = 200
        return output


@tablet_server.route('/api/tables/<path:text>', methods=['GET'])
def get_particular_info(text):
    if text not in table_contents.keys():
        return Response(status=404)
    table_info = table_contents[text]
    response_dict = jsonify(table_info)
    response_dict.status_code = 200
    return response_dict


@tablet_server.route('/api/tables/<path:text>', methods=['DELETE'])
def table_delete(text):
    global tables_list
    if len(tables_list) == 0:
        return Response(status=400)
    else:
        if str(text) in tables_list:
            tables_list.remove(text)
            del table_contents[text]
            return Response(status=200)
        else:
            return Response(status=404)


def spill(content):
    pass


def run_gc(row_key):
    print("Came into garbage collection")
    # pdb.set_trace()
    index_to_be_del = 0
    for each_entry in mem_table:
        var = each_entry['row']
        if var == row_key:
            del mem_table[index_to_be_del]
            row_counter[row_key] -= 1
            return
        index_to_be_del += 1


def update_lru_counter(content):
    row_key = content['row']
    if row_key in row_counter.keys():
        row_counter[row_key] += 1
    else:
        row_counter[row_key] = 1
    if row_counter[row_key] > 5:
        run_gc(row_key)


def add_row_to_mem_table(table_name, content):
    write_ahead_log_entry(content, table_name)
    global mem_table
    global mem_table_spill_counter
    print(content)
    # import pdb;
    # pdb.set_trace()
    update_lru_counter(content)
    if len(mem_table) == 0:
        if mem_table_spill_counter == 0:
            create_meta_data_file(content, table_name)
            mem_table.append(content)
            return Response(status=200)
    elif len(mem_table) < mem_table_size:
        mem_table.append(content)
        sorted(mem_table, key=lambda x: (str(x["data"][0]["value"]), float(x["data"][0]["time"])))
    elif len(mem_table) == mem_table_size:
        spill(content)
        mem_table = list()
        mem_table_spill_counter += 1
    return Response(status=200)


def create_meta_data_file(content, table_name):
    meta_data = persistent_storage + table_name + ".mdt"
    file_desc = open(meta_data, 'a+')
    col_fam = content['column_family']
    file_desc.write("Column Family:" + col_fam + "\n")
    col_name = content['column']
    file_desc.write("\tColumn: " + col_name + "\n")
    file_desc.close()


def write_ahead_log_entry(content, table_name):
    wal = persistent_storage + table_name + ".wal"
    file_desc = open(wal, 'a+')
    file_desc.write(json.dumps(content))
    file_desc.close()


def check_col_fam_exists(table_name, col_fam):
    global table_contents
    table_infor = table_contents[table_name]
    list_of_families = table_infor['column_families']
    found = False
    for each_col_fam in list_of_families:
        if each_col_fam['column_family_key'] == col_fam:
            found = True
            return found
    return found


def check_col_exists(table_name, col_fam, col_name):
    global table_contents
    table_infor = table_contents[table_name]
    list_of_families = table_infor['column_families']
    found = False
    for each_col_fam in list_of_families:
        if each_col_fam['column_family_key'] == col_fam:
            all_col = each_col_fam['columns']
            if col_name in all_col:
                found = True
                return found
    return found


@tablet_server.route('/api/table/<path:text>/cell', methods=['POST'])
def insert_a_cell(text):
    # import pdb; pdb.set_trace()
    # print(text)
    # if text == "table_gc":
    # pdb.set_trace()
    if text not in tables_list:
        return Response(status=404)
    content = request.get_json()
    col_fam = content['column_family']
    if not check_col_fam_exists(text, col_fam):
        return Response(status=400)
    col = content['column']
    if not check_col_exists(text, col_fam, col):
        return Response(status=400)
    return add_row_to_mem_table(text, content)


def find_a_row_memt(table, row_num):
    result = list()
    for row in mem_table:
        if row['row'] == row_num:
            result.append(row)
            # return result
    return result


def get_row_from_mem_table(text, content):

    row_name = content['row']
    row = find_a_row_memt(text, row_name)
    # import pdb; pdb.set_trace()
    data_key_list = list()
    if len(row) != 0:
        for each_ent in row:
            if len(row) == 1:
                output_range = dict()
                output_range['row'] = each_ent['row']
                output_range['data'] = each_ent['data']
                send_single_out = jsonify(output_range)
                send_single_out.status_code = 200
                return send_single_out
            else:
                data_key_list.append(get_multiple_row_value(each_ent))
        new_dict = dict()
        new_dict["row"] = row_name
        new_dict["data"] = data_key_list
        send_single_output = jsonify(new_dict)
        send_single_output.status_code = 200
        return send_single_output
    else:
        return Response(status=400)


def get_multiple_row_value(each_ent):
    each_val = each_ent['data'][0]['value']
    new_diction = dict()
    new_diction['value'] = each_val
    return new_diction


@tablet_server.route('/api/table/<path:text>/cell', methods=['GET'])
def retrieve_a_cell(text):
    content = request.get_json()
    # if text == "table_gc":
    #     pdb.set_trace()
    tbl_name = text
    if tbl_name not in tables_list:
        return Response(status=404)
    else:
        send_result = get_row_from_mem_table(text, content)
        return send_result


'''
Search the row in the memtable and return the index 
if found. If not found, then return the None 
'''


def retrieve_cell_index_memt(row_from, row_to, direction):
    if direction == "front":
        indx = 0
        for each_row in mem_table:
            if each_row['row'] >= row_from:
                return indx
            indx += 1
    else:
        for i in range(len(mem_table) - 1, -1, -1):
            if mem_table[i]['row'] <= row_to:
                return i
    return None


def retrieve_range_of_cells_memt(start_index, end_index):
    result_list = list()
    # import pdb; pdb.set_trace()
    for each_row in range(start_index, end_index + 1):
        output_range = dict()
        output_range['row'] = mem_table[each_row]['row']
        output_range['data'] = mem_table[each_row]['data']
        result_list.append(output_range)
    send_range = dict()
    send_range['rows'] = result_list
    # send_range.status_code = 200
    return send_range


def get_range_rows_mem_table(table_name, row_from, row_to):
    range_result = dict()
    if table_name not in tables_list:
        return Response(status=404)
    content = request.get_json()
    # import pdb;
    # pdb.set_trace()
    start_index = retrieve_cell_index_memt(content['row_from'], content['row_to'], "front")
    end_index = retrieve_cell_index_memt(content['row_from'], content['row_to'], "reverse")
    if start_index is not None and end_index is not None:
        return retrieve_range_of_cells_memt(start_index, end_index)
    return Response(status=200)


@tablet_server.route('/api/table/<path:text>/cells', methods=['GET'])
def retrieve_range_of_cells(text):
    content = request.get_json()
    row_from = content['row_from']
    row_to = content['row_to']
    response = jsonify(get_range_rows_mem_table(text, row_from, row_to))
    response.status_code = 200
    return response


@tablet_server.route('/api/memtable', methods=['GET'])
def set_mem_table_max_entries():
    global mem_table_size
    mem_table_size = request.args.get('memtable_max')
    return Response(status=200)


