import flask
import requests
from flask import request, jsonify, Response
import json
import pdb
import sys
import os
import socket

tablet_server = flask.Flask(__name__)
tablet_server.config["DEBUG"] = False
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
row_major = False
col_major = False
lru_limit = 5

"""
    sharding limit for rows 
"""
sharding_limit = 1000

''' counter for number of rows in one table '''
table_rows_count = {}

''' row_from for this tablet server '''
row_from = {}

''' row_to for this tablet server against table name'''
row_to = {}
"""max int """
maximum = sys.maxsize


def table_row_major(table_info):
    col_fam_list = table_info['column_families']
    num_columns = 0
    lru = 5
    global row_major, mem_table_size
    for each_col_fam in col_fam_list:
        num_columns += len(each_col_fam['columns'])
    if num_columns > lru:
        row_major = True
        mem_table_size += 2
        lru = num_columns
    return lru


def create_table_self(table_name, table_info):
    global lru_limit, col_major
    path = table_name + ".mdt"
    file_desc = open(path, 'w')
    file_desc.write(json.dumps(table_info))
    file_desc.close()

    tables_list.append(table_name)
    table_contents[table_name] = table_info
    # if row_major:
    #     col_major = True
    lru_limit = table_row_major(table_info)
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


@tablet_server.route('/api/shard/copyinmem', methods=['POST'])
def copyinmem():
    content = request.get_json()
    global in_memory_index
    in_memory_index = content['in_mem_index']
    return Response(status=200)


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
    global lru_limit
    if row_key in row_counter.keys():
        row_counter[row_key] += 1
    else:
        row_counter[row_key] = 1
    if row_counter[row_key] > lru_limit:
        print("row key more than 5")
        run_gc(row_key)


def add_row_to_mem_table(table_name, content):
    global mem_table
    global mem_table_spill_counter
    global this_spill_list
    write_ahead_log_entry(content, table_name)

    if not row_major:
        update_lru_counter(content)
    if len(mem_table) == 0:
        mem_table.append(content)
        this_spill_list.append(str(table_name + "|" + str(content['row']) + "|" + str(content['column'])))
        return Response(status=200)
    elif len(mem_table) < mem_table_size:
        mem_table.append(content)
        sorted(mem_table, key=lambda x: (float(x["data"][0]["time"])))
        this_spill_list.append(str(table_name + "|" + str(content['row']) + "|" + str(content['column'])))
    elif len(mem_table) >= mem_table_size:
        mem_table_spill(table_name, content)
    return Response(status=200)


"""
    Method to spill the mem_table to disk. 
    This will be triggered during normal row insertion as 
    well as the change in the mem_table_limit 
"""


def mem_table_spill(table_name, content):
    global mem_table, this_spill_list, mem_table_spill_counter, table_spill_dict
    if len(mem_table) < mem_table_size:
        return
    spill(mem_table)
    mem_table = list()
    table_spill_dict[mem_table_spill_counter] = this_spill_list
    this_spill_list = list()
    in_memory_index.append(table_spill_dict)
    table_spill_dict = dict()
    mem_table.append(content)
    this_spill_list.append(str(table_name + "|" + str(content['row']) + "|" + str(content['column'])))
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


def shard_tablet_server(table_name):
    with open('hosts.mk', 'r') as hosts_file:
        data = hosts_file.readlines()
    strings = data[0].split("=")
    master_hostname = strings[1][:-1]
    master_port = (data[1].split("="))[1]
    master_url = "http://" + master_hostname + ":" + master_port + "/api/shard/shardtab/" + sys.argv[2]
    response = requests.get(master_url)
    content = response.json()
    shard_tab_ip = content['shard_hostname']
    shard_tab_port = content['shard_port']
    # pdb.set_trace()
    output_dict = {'in_mem_index': in_memory_index}
    # in_mem_json = json.dumps(in_memory_index)
    table_rows_count[table_name] -= 500
    table_dict = table_contents[table_name]
    table_create_url = "http://" + shard_tab_ip + ":" + shard_tab_port + "/api/tables"
    response = requests.post(table_create_url, json=table_dict)
    shard_tab_url = "http://" + shard_tab_ip + ":" + shard_tab_port + "/api/shard/copyinmem"
    response = requests.post(shard_tab_url, json=output_dict)
    sharded_information = {
        "tablet_key": shard_tab_port,
        "table_name": table_name,
        "row_from_dest": 0,
        "row_to_dest": sharding_limit // 2,
        "row_from_orig": sharding_limit // 2,
        "row_to_orig": maximum
    }
    requests.post(master_url, json=sharded_information)


@tablet_server.route('/api/table/<path:text>/cell', methods=['POST'])
def insert_a_cell(text):
    content = request.get_json()
    global table_rows_count
    if text not in tables_list:
        return Response(status=404)
    col_fam = content['column_family']
    if not check_col_fam_exists(text, col_fam):
        return Response(status=400)
    col = content['column']
    if not check_col_exists(text, col_fam, col):
        return Response(status=400)
    if text not in table_rows_count.keys():
        table_rows_count[text] = 1
    else:
        table_rows_count[text] += 1
    if not row_major and table_rows_count[text] >= sharding_limit:
        shard_tablet_server(text)
    return add_row_to_mem_table(text, content)


def find_a_row_memt(table, row_num, col_name):
    result = list()
    global row_major
    for row in mem_table:
        if row['row'] == row_num and not row_major and row['column'] == col_name:
            result.append(row)
        elif row['row'] == row_num and row_major and row['column'] == col_name:
            return row
    return result


def find_value_on_ss_index(ss_index_val, row_name, table, col_name):
    global row_major
    key_val = "sstable_" + str(ss_index_val)
    ss_table_name = ss_index[key_val]
    int_row_count = 1
    lines = [line.rstrip('\n') for line in open(ss_table_name)]
    result = list()
    strs = lines[0].split("||")
    for each_line in strs:
        if each_line != "":
            data = json.loads(each_line)
            if data['row'] == row_name and not row_major:
                result.append(data)
            elif row_major and data['row'] == row_name and data['column'] == col_name:
                return data
    return result


str_line = str


def find_a_row_on_disk(table, row_name, col_name):
    global str_line
    ss_index_val = 0
    # check where the row exists: i.e. in
    # which ss_table
    # if table == "table_shard":
    #     pdb.set_trace()
    for each_dict in in_memory_index:
        for each_list in each_dict.values():
            for each_entry in each_list:
                str_list = each_entry.split("|")
                if str_list[1] == str(row_name) and str_list[0] == table and str_list[2] == col_name:
                    result_list = find_value_on_ss_index(ss_index_val, row_name, table, col_name)
                    return result_list
        ss_index_val += 1


def find_col_exists(table_name, content):
    table_info = table_contents[table_name]['column_families']
    for each_col_family in table_info:
        if each_col_family['column_family_key'] == content['column_family']:
            column_list = each_col_family['columns']
            if content['column'] not in column_list:
                return False
    return True


str_line_col = str


def find_value_on_ss_index_col_maj(ss_index_val, row_name, col_name):
    key_val = "sstable_" + str(ss_index_val)
    ss_table_name = ss_index[key_val]
    lines = [line.rstrip('\n') for line in open(ss_table_name)]
    result = list()
    strs = lines[0].split("||")
    for each_line in strs:
        if each_line != "":
            data = json.loads(each_line)
            if data['row'] == row_name and data['column'] == col_name:
                return data


def find_data_col_maj(text, row_name, col_name):
    global col_num_count, current_col
    global str_line_col
    ss_index_val = 0
    # check where the row exists: i.e. in
    # which ss_table
    for each_dict in in_memory_index:
        for each_list in each_dict.values():
            for each_entry in each_list:
                str_line_col = each_entry.split("|")
                if str_line_col[1] == str(row_name) and str_line_col[0] == text:
                    result_list = find_value_on_ss_index_col_maj(ss_index_val, row_name, col_name)
                    return result_list
        ss_index_val += 1


def get_row_from_mem_table_disk(text, content):
    # if text == "movies":
    #     pdb.set_trace()
    global row_major
    row_name = content['row']
    col_name = content['column']
    if col_major:
        row = find_data_col_maj(text, row_name, col_name)
    else:
        row = find_a_row_memt(text, row_name, col_name)
    if row is None:
        row_major = False
        row = find_a_row_memt(text, row_name, col_name)
    # if text == "table_rcvr" and row is None:
    #     pdb.set_trace()
    if len(row) == 0 and not find_col_exists(text, content):
        return Response(status=400)

    # did not find on mem table- so search in ss index / table
    if len(row) == 0:
        row = find_a_row_on_disk(text, row_name, col_name)

    data_key_list = list()
    if len(row) != 0:
        if type(row) == dict:
            out_range = dict()
            out_range['row'] = row['row']
            out_range['data'] = row['data']
            send_single_out = jsonify(out_range)
            send_single_out.status_code = 200
            return send_single_out
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
    content = request.get_json()
    # recovery when mem table is 0 and tables list is empty
    # reasoning for these conditions for recovery is that
    # you cannot retrieve anything if there is no table
    # so attempt to recover data from the meta data and wal
    if len(mem_table) == 0 and len(tables_list) == 0:
        recovered = recover_from_md(text)
    tbl_name = text
    # if text == "movies":
    #     pdb.set_trace()
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
    return send_range


string_line = str


def find_row_on_disk(table_name, row_name):
    global string_line
    ss_index_val = 0
    # check where the row exists: i.e. in
    # which ss_table
    for each_dict in reversed(in_memory_index):
        for each_list in each_dict.values():
            for each_entry in each_list:
                str_list = each_entry.split("|")
                if str_list[1] == row_name and str_list[0] == table_name:
                    return ss_index_val
        ss_index_val += 1


def find_range_of_values_on_sstable(ss_index_val, row_from, row_to):
    # take the file name where the values exists
    key_val = "sstable_" + str(ss_index_val)
    ss_table_name = ss_index[key_val]

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


@tablet_server.route('/api/tablethb', methods=['GET'])
def send_heartbeat():
    return Response(status=200)


@tablet_server.route('/api/recover', methods=['POST'])
def start_recovery():
    content = request.get_json()
    recovery_host = content['hostname']
    recovery_port = content['port']
    table_dict = content['tables_information']
    table_create_url = "http://" + recovery_host + ":" + recovery_port + "/api/tables"
    response = requests.post(table_create_url, json=table_dict)
    table_name = table_dict['name']
    recovered = recover_from_md(table_name)
    if recovered:
        return Response(status=200)
    else:
        return Response(status=400)


@tablet_server.route('/api/memtable', methods=['POST'])
def set_mem_table_max_entries():
    global mem_table_size
    content = request.get_json()
    mem_table_size = content['memtable_max']
    return Response(status=200)


""" api for sharding constant """


@tablet_server.route('/api/sharding_limit', methods=['POST'])
def set_sharding_limit():
    global sharding_limit
    content = request.get_json()
    sharding_limit = content['sharding_limit']
    return Response(status=200)


ipaddr = ""

if __name__ == '__main__':
    tablet_ipaddress = socket.gethostbyname(socket.gethostname())
    tablet_port_num = sys.argv[2]
    tablet_information = {
        "ipaddress": tablet_ipaddress,
        "port": tablet_port_num
    }
    # get the master ip and port number
    master_ipaddress = sys.argv[3]
    master_portnum = sys.argv[4]
    master_url = "http://" + master_ipaddress + ":" + master_portnum + "/api/updatetabletdetails"
    requests.post(master_url, json=tablet_information)
    ipaddr = tablet_ipaddress
    lines = [line.rstrip('\n') for line in open('hosts.mk')]
    tablet_host_name = ""
    index = 0
    if tablet_port_num == "19192":
        tablet_host_name = "TABLET1_HOSTNAME"
    elif tablet_port_num == "19193":
        tablet_host_name = "TABLET2_HOSTNAME"
    elif tablet_port_num == "19194":
        tablet_host_name = "TABLET3_HOSTNAME"
    for each_l in lines:
        for each_line in each_l:
            strin = each_l.split("=")
            if strin[0] == tablet_host_name:
                strin[1] = ipaddr
                tablet_host_name = strin[0] + "=" + strin[1] + "\n"
                break
            index += 1

    with open('hosts.mk', 'r') as file:
        data = file.readlines()

    data[index] = tablet_host_name

    with open('hosts.mk', 'w') as file:
        file.writelines(data)
    # print("length of data" + str(len_data))
    # now change the 2nd line, note that you have to add a newline
    tablet_server.run(host=ipaddr, port=int(sys.argv[2]))
