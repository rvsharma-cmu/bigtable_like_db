import flask
from flask import request, jsonify, Response
import json
import pdb
import sys
import os


"""
    Max LRU limit that updates on the basis 
    of the columns in the table 
"""
lru_limit = 5

tablet_server = flask.Flask(__name__)
tablet_server.config["DEBUG"] = True
""" tablet to server name mapping
    This will list which tablet is 
    mapped to which server. 
"""
tablet_serv_name_mapping = dict()

'the list of tables in this tablet server'
tables_list = list()

'dictionary for the ss tables and indexes of the ss tables'
ss_index = dict()

""" Dictionary for table. 
    It will have the properties of table like column name and rows
    Essentially it will contain the json input when the table is being created
"""
table_contents = dict()

"""
    This is the memtable for the tablet server
"""
mem_table = list()

'default size of the memtable. Initial value is 100'
mem_table_size = 100
"""mem_table spill counter. The number of times that this
    has spilled to disk
"""
mem_table_spill_counter = 0
"""
dictionary of rows against the counts of values inserted 
"""
row_counter = dict()
'in memory index'
in_memory_index = []
'list of all rows that exist in this mem_table spillage'
this_spill_list = []
"""
    Dictionary that maps the spill list with the 
    index of the SS table where the rows information 
    is stored
"""
table_spill_dict = dict()
"""
    Metadata hash set to track if metadata has been created for this table. 
    This just has the table names of tables which have their metadata stored 
    on the disk in case of recovery 
"""
metadata_exists = set()

"""
    Method that appends a table name to the 
    list of tables for this server
"""


def count_columns(table_info):
    col_fams = table_info['column_families']
    columns = 0
    for each_col_fam in col_fams:
        columns += len(each_col_fam['columns'])
    return columns


def create_table_self(table_name, table_info):
    global lru_limit
    path = table_name + ".mdt"
    file_desc = open(path, 'w')
    file_desc.write(json.dumps(table_info))
    file_desc.close()

    # write_ahead_log = table_name + ".wal"
    # file_desc = open(write_ahead_log, 'w')
    # file_desc.write("Table:" + table_name + "\n")
    # file_desc.close()
    tables_list.append(table_name)
    table_contents[table_name] = table_info
    val = count_columns(table_info)
    if val > 5:
        lru_limit = val
    return Response(status=200)


"""
    API to create a table and return appropriate response
    :return 400 status if cannot parse json 
    :return 409 if the table already exists 
    :return 200 if successfully created table 
"""


@tablet_server.route('/api/tables', methods=['POST'])
def create_table():
    # create a table with name
    if request.method == 'POST':
        table_info = request.get_json()
        if table_info is None:
            return Response(status=400)
        for server_name in tables_list:
            # print(server_name)
            # return 409 if table already exists
            if table_info['name'] == server_name:
                return Response(status=409)
        # else create a table with request

        if table_info['name'] not in tables_list:
            response = create_table_self(table_info['name'], table_info)
            return response


"""
    List all tables that are present on this tablet
    : return 200 and content with the table info 
    : return 200 with empty content if no tables are defined 
"""


@tablet_server.route('/api/tables', methods=['GET'])
def list_tables():
    global tables_list
    response = dict()
    table_names = list()
    output = dict()
    # send empty response if no tables
    # are defined
    if len(tables_list) == 0:
        response["tables"] = table_names
        output = jsonify(response)
        output.status_code = 200
        return output
    else:
        for table_n in tables_list:
            table_names.append(table_n)
        response["tables"] = table_names
        output = jsonify(response)
        output.status_code = 200
        return output


"""
    This api handle gets all the information 
    of the associated table to the caller 
    Values like columns, column families, 
    table name 
"""


@tablet_server.route('/api/tables/<path:text>', methods=['GET'])
def get_particular_info(text):
    if text not in table_contents.keys():
        return Response(status=404)
    table_info = table_contents[text]
    response_dict = jsonify(table_info)
    response_dict.status_code = 200
    return response_dict


"""
    This method deletes a particular table from the 
    tablet server 
"""


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


def spill(mem_table_to_spill):
    path = "sstable_ " + str(mem_table_spill_counter) + ".txt"
    ss_index['sstable_' + str(mem_table_spill_counter)] = path
    file_desc = open(path, 'w+')
    for each_entry in mem_table_to_spill:
        file_desc.write(json.dumps(each_entry) + "||")
    file_desc.close()
    # table_spill_dict = dict()
    # table_spill_dict[mem_table_spill_counter] = []


def run_gc(row_key):
    print("Came into garbage collection")
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
    if row_counter[row_key] > lru_limit:
        run_gc(row_key)


def add_row_to_mem_table(table_name, content):
    global mem_table
    global mem_table_spill_counter
    global this_spill_list
    write_ahead_log_entry(content, table_name)
    # print(content)
    update_lru_counter(content)
    if len(mem_table) == 0:
        # if table_name not in metadata_exists:
        #     create_meta_data_file(content, table_name)
        mem_table.append(content)
        this_spill_list.append(str(table_name + "|" + str(content['row'])))
        return Response(status=200)
    elif len(mem_table) < mem_table_size:
        mem_table.append(content)
        sorted(mem_table, key=lambda x: (float(x["data"][0]["time"])))
        this_spill_list.append(str(table_name + "|" + str(content['row'])))
    elif len(mem_table) >= mem_table_size:
        mem_table_spill()
    return Response(status=200)


"""
    Method to spill the mem_table to disk. 
    This will be triggered during normal row insertion as 
    well as the change in the mem_table_limit 
"""


def mem_table_spill():
    global mem_table, this_spill_list, mem_table_spill_counter, table_spill_dict
    if len(mem_table) < mem_table_size:
        return
    spill(mem_table)
    mem_table = list()
    table_spill_dict[mem_table_spill_counter] = this_spill_list
    this_spill_list = list()
    in_memory_index.append(table_spill_dict)
    table_spill_dict = dict()
    mem_table_spill_counter += 1


def create_meta_data_file(content, table_name):
    meta_data = table_name + ".mdt"
    file_desc = open(meta_data, 'a+')
    file_desc.write(json.dumps(content))
    file_desc.close()


def write_ahead_log_entry(content, table_name):
    wal = table_name + ".wal"
    file_desc = open(wal, 'a+')
    file_desc.write(json.dumps(content) + "\n")
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


def find_value_on_ss_index(ss_index_val, row_name, table):
    key_val = "sstable_" + str(ss_index_val)
    ss_table_name = ss_index[key_val]
    # file_desc = open(ss_table_name, 'r')
    # file_desc.read()
    lines = [line.rstrip('\n') for line in open(ss_table_name)]
    result = list()
    strs = lines[0].split("||")
    for each_line in strs:
        if each_line != "":
            data = json.loads(each_line)
            if data['row'] == row_name:
                result.append(data)
                # return data
    return result


str_line = str


def find_a_row_on_disk(table, row_name):
    global str_line
    ss_index_val = 0
    # check where the row exists: i.e. in
    # which ss_table
    for each_dict in in_memory_index:
        for each_list in each_dict.values():
            for each_entry in each_list:
                str_list = each_entry.split("|")
                if str_list[1] == str(row_name) and str_list[0] == table:
                    result_list = find_value_on_ss_index(ss_index_val, row_name, table)
                    return result_list
        ss_index_val += 1
        # print(str_line)


def find_col_exists(table_name, content):
    table_info = table_contents[table_name]['column_families']
    for each_col_family in table_info:
        if each_col_family['column_family_key'] == content['column_family']:
            column_list = each_col_family['columns']
            if content['column'] not in column_list:
                return False
    return True


def get_row_from_mem_table_disk(text, content):
    row_name = content['row']
    row = find_a_row_memt(text, row_name)
    if len(row) == 0 and not find_col_exists(text, content):
        return Response(status=400)
    # did not find on mem table- so search in ss index / table
    if len(row) == 0:
        row = find_a_row_on_disk(text, row_name)
    data_key_list = list()
    if len(row) != 0:
        if type(row) == dict:
            row_dict = dict()
            row_dict["row"] = row_name
            row_dict["data"] = row['data']
            send_out = jsonify(row_dict)
            send_out.status_code = 200
            return send_out
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
    # return 400 if not found anywhere
    else:
        return Response(status=400)


def get_multiple_row_value(each_ent):
    each_val = each_ent['data'][0]['value']
    new_diction = dict()
    new_diction['value'] = each_val
    return new_diction


def recover_from_md(table_name):
    meta_data_file_name = table_name + ".mdt"
    wal_file_name = table_name + ".wal"
    cwd = os.getcwd()
    recovered = False

    for root, dirs, files in os.walk(cwd):
        if meta_data_file_name in files and wal_file_name in files:
            # os.path.join(root, meta_data_file_name)
            # file_desc = open(meta_data_file_name, 'r+')
            # get the table infor from the meta data file
            lines = [line.rstrip('\n') for line in open(meta_data_file_name)]
            for each_l in lines:
                data = json.loads(each_l)
                # and create the table in the tables list
                create_table_self(table_name, data)
            # use the wal to send the row data one by one
            lines = [line.rstrip('\n') for line in open(wal_file_name)]
            for each_line in lines:
                add_row_to_mem_table(table_name, json.loads(each_line))
            recovered = True
    return recovered


@tablet_server.route('/api/table/<path:text>/cell', methods=['GET'])
def retrieve_a_cell(text):
    if text == "my_csv":
        pdb.set_trace()
    # recovery when mem table is 0 and tables list is empty
    # reasoning for these conditions for recovery is that
    # you cannot retrieve anything if there is no table
    # so attempt to recover data from the meta data and wal
    if len(mem_table) == 0 and len(tables_list) == 0:
        recovered = recover_from_md(text)
    content = request.get_json()
    tbl_name = text
    if tbl_name not in tables_list:
        return Response(status=404)
    else:
        send_result = get_row_from_mem_table_disk(tbl_name, content)
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
    for each_row in range(start_index, end_index + 1):
        output_range = dict()
        output_range['row'] = mem_table[each_row]['row']
        output_range['data'] = mem_table[each_row]['data']
        result_list.append(output_range)
    send_range = dict()
    send_range['rows'] = result_list
    # send_range.status_code = 200
    return send_range


string_line = str


def find_row_on_disk(table_name, row_name):
    global string_line
    ss_index_val = 0
    # check where the row exists: i.e. in
    # which ss_table
    # for i in range(len(in_memory_index))
    for each_dict in reversed(in_memory_index):
        for each_list in each_dict.values():
            for each_entry in each_list:
                str_list = each_entry.split("|")
                if str_list[1] == row_name and str_list[0] == table_name:
                    return ss_index_val
        ss_index_val += 1
        # print(str_line)


def find_range_of_values_on_sstable(ss_index_val, row_from, row_to):
    # take the file name where the values exists
    key_val = "sstable_" + str(ss_index_val)
    ss_table_name = ss_index[key_val]
    # file_desc = open(ss_table_name, 'r')
    # file_desc.read()
    lines = [line.rstrip('\n') for line in open(ss_table_name)]
    result = list()
    strs = lines[0].split("||")
    for each_line in strs:
        if each_line != "":
            data = json.loads(each_line)
            if row_from <= data['row'] <= row_to:
                result.append(data)
    return result


def retrieve_range_of_cells_sstable(table_name, row_from, row_to):
    # reversed loop;
    row_from_index_num = find_row_on_disk(table_name, row_from)
    row_to_index_num = find_row_on_disk(table_name, row_to)
    range_list = list()
    if row_from_index_num == row_to_index_num:
        range_list = find_range_of_values_on_sstable(len(in_memory_index) - row_from_index_num - 1, row_from, row_to)
    result_list = list()
    for each_row in range_list:
        output_range = dict()
        output_range['row'] = each_row['row']
        output_range['data'] = each_row['data']
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
    start_index = retrieve_cell_index_memt(content['row_from'], content['row_to'], "front")
    end_index = retrieve_cell_index_memt(content['row_from'], content['row_to'], "reverse")
    # check if the start and end row exists in mem table
    if start_index is not None and end_index is not None:
        return retrieve_range_of_cells_memt(start_index, end_index)
    # if both indexes are none then check in ss tables
    elif start_index is None and end_index is None:
        return retrieve_range_of_cells_sstable(table_name, row_from, row_to)


@tablet_server.route('/api/table/<path:text>/cells', methods=['GET'])
def retrieve_range_of_cells(text):

    content = request.get_json()
    row_from = content['row_from']
    row_to = content['row_to']
    rows_list = get_range_rows_mem_table(text, row_from, row_to)
    response = jsonify(rows_list)
    response.status_code = 200
    return response


@tablet_server.route('/api/memtable', methods=['POST'])
def set_mem_table_max_entries():
    global mem_table_size
    content = request.get_json()
    mem_table_size = content['memtable_max']
    mem_table_spill()
    return Response(status=200)


tablet_server.run(host=sys.argv[1], port=sys.argv[2])
