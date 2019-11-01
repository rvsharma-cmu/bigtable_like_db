import copy

import flask
import requests
import urllib3
from flask import request, jsonify, Response
import sys
import pdb
import json
import threading
import time

master_server = flask.Flask(__name__)
master_server.config["DEBUG"] = True
"""
    dict containing the mapping of ports to hostname
"""
tablet_dict = dict()
"""
    dict of table names against tablets
"""
tablet_table_dict = dict()
"""
    copy of tablets 
"""
copy_tablet_table_dict = dict()
"""
    dict of tables in use where the key is table name and  
    values are the client id who hold the lock 
"""
open_list = dict()
tablet_deleted_key = ""
r = dict()
'''list of tables that are to be recovered'''
list_of_tabs = list()


def collect_tables_from_tablets():
    global tablet_table_dict, copy_tablet_table_dict
    tables_list = list()
    for each_tablet in tablet_dict.keys():
        url = "http://" + "localhost" + ":" + str(each_tablet) + "/api/tables"
        response = requests.get(url).content
        val = json.loads(response)
        list_of_tables = val['tables']
        tablet_table_dict[each_tablet] = list_of_tables
        copy_tablet_table_dict[each_tablet] = list_of_tables
        for each_table in list_of_tables:
            tables_list.append(str(each_table))
    return tables_list


@master_server.route('/api/tables', methods=['GET'])
def master_list_tables():
    # no tables are defined
    resp_list = list()
    if len(tablet_dict) == 0:
        resp_dict = dict()
        resp_dict['tables'] = resp_list
        response_send = jsonify(resp_dict)
        response_send.status_code = 200
        return response_send
    else:
        resp_list = collect_tables_from_tablets()
        resp_dict = dict()
        resp_dict['tables'] = resp_list
        response_send = jsonify(resp_dict)
        response_send.status_code = 200
        return response_send


@master_server.route('/api/tables/<path:text>/', methods=['GET'], strict_slashes=False)
@master_server.route('/api/tables/<path:text>', methods=['GET'], strict_slashes=False)
def get_particular_table_info(text):
    if text == "table_rcvr":
        pdb.set_trace()
    list_of_all_tables = collect_tables_from_tablets()
    if text not in list_of_all_tables:
        return Response(status=404)

    for each_tablet_key in tablet_table_dict:
        if text in tablet_table_dict[each_tablet_key]:
            connect_url = "http://" + "localhost" + ":" + str(each_tablet_key) + "/api/tables/" + text
            response = requests.get(connect_url).content
            otp = json.loads(response)
            tablet_list = list()
            tablet_info = dict()
            tablet_info["hostname"] = tablet_dict[each_tablet_key]
            tablet_info["port"] = each_tablet_key
            tablet_list.append(tablet_info)
            otp["tablets"] = tablet_list
            output_json = jsonify(otp)
            output_json.status_code = 200
            return output_json


def check_if_table_exists(table_name):
    all_tables_list = collect_tables_from_tablets()
    if table_name in all_tables_list:
        return True
    return False


def load_balance_tablet():
    # pdb.set_trace()
    collect_tables_from_tablets()
    length = sys.maxsize
    smallest_tablet = ""
    for each_tablet in tablet_table_dict.keys():
        this_len = len(tablet_table_dict[each_tablet])
        if this_len < length:
            smallest_tablet = each_tablet
            length = this_len
    return smallest_tablet


def create_a_table_given_tablet(tablet_serv_key, content):
    tablet_url = "http://" + "localhost" + ":" + str(tablet_serv_key) + "/api/tables"
    response = requests.post(tablet_url, None, content)
    if response.status_code == 200:
        return True
    else:
        return False


@master_server.route('/api/lock/<path:text>', methods=['POST'])
def lock_table(text):
    # pdb.set_trace()
    all_tables = collect_tables_from_tablets()
    if text not in all_tables:
        return Response(status=404)
    content = request.get_json()
    client_id = content['client_id']
    if len(open_list) == 0:
        new_set = set()
        new_set.add(client_id)
        open_list[text] = new_set
        return Response(status=200)
    else:
        for each_table in open_list:
            if text == each_table:
                client_id_set = open_list[each_table]
                if client_id in client_id_set:
                    return Response(status=400)
                else:
                    client_id_set.add(client_id)
                    open_list[each_table] = client_id_set
                    return Response(status=200)
            else:
                new_set = set()
                new_set.add(client_id)
                open_list[text] = new_set
                return Response(status=200)


@master_server.route('/api/lock/<path:text>', methods=['DELETE'])
def unlock_table(text):
    # pdb.set_trace()
    content = request.get_json()
    client_id = content['client_id']
    all_tables = collect_tables_from_tablets()
    if text not in all_tables:
        return Response(status=404)
    # if there are no tables locked return 400 immediately
    if len(open_list) == 0:
        return Response(status=400)
    # else find the client id and delete if it exists
    for each_table in open_list:
        # if client did not open the table
        if client_id not in open_list[each_table]:
            return Response(status=400)
        else:
            new_set = open_list[each_table]
            new_set.remove(client_id)
            if len(new_set) == 0:
                del open_list[each_table]
            return Response(status=200)


@master_server.route('/api/tables', methods=['POST'])
def master_create_a_table():
    content = request.get_json()
    if content is None:
        return Response(status=400)
    table_name = content['name']
    if check_if_table_exists(table_name):
        return Response(status=409)
    tablet_serv_key = load_balance_tablet()
    if create_a_table_given_tablet(tablet_serv_key, content):
        output_dict = dict()
        output_dict['hostname'] = tablet_dict[tablet_serv_key]
        output_dict['port'] = tablet_serv_key
        response_value = jsonify(output_dict)
        response_value.status_code = 200
        return response_value


@master_server.route('/api/tables/<path:text>', methods=['DELETE'])
def table_delete(text):
    # pdb.set_trace()
    list_of_tables = collect_tables_from_tablets()
    if text not in list_of_tables:
        return Response(status=404)
    if text in open_list.keys():
        return Response(status=409)
    list_of_tablets = list()
    for each_tab in tablet_table_dict:
        if text in tablet_table_dict[each_tab]:
            list_of_tablets.append(each_tab)
    for each_tab in list_of_tablets:
        tablet_url = "http://" + "localhost" + ":" + str(each_tab) + "/api/tables/" + text
        var = requests.delete(tablet_url)
        if var.status_code == 200:
            continue
        else:
            return Response(status=409)
    return Response(status=200)


def remove_tablet_server(each_tablet):
    print("in remove tablet server")
    global list_of_tabs
    if each_tablet in tablet_dict.keys():
        del tablet_dict[each_tablet]
        print("copying list of tabs")
        list_of_tabs = tablet_table_dict[each_tablet]
        del tablet_table_dict[each_tablet]


def do_recovery(each_tablet):
    print("in recovery")


# def heartbeat():
#     global tablet_table_dict, r, tablet_deleted_key
#     timer = threading.Timer(5.0, heartbeat)
#     timer.start()
#     # pdb.set_trace()
#     tables_list = list()
#     for each_tablet in tablet_dict.keys():
#         url = "http://" + "localhost" + ":" + str(each_tablet) + "/api/tablethb"
#         try:
#             r = requests.get(url)
#         except requests.exceptions.ConnectionError:
#             tablet_deleted_key = each_tablet
#             remove_tablet_server(each_tablet)
#             do_recovery(each_tablet)
#         except urllib3.exceptions.NewConnectionError:
#             print("excp handled")
#         except urllib3.exceptions.MaxRetryError:
#             print("excp handled")
#         except ConnectionRefusedError:
#             print("excp handled")
#
#         print("Response=")
#         print(r)
#         stat_code = r.status_code
#         if stat_code == 200:
#             print("Alive " + str(each_tablet))
#         print(tablet_deleted_key)

def heartbeat():
    recovered = []
    while True:
        tablets_copy = copy.copy(tablet_dict)
        for each_tablet in tablets_copy.keys():
            url = "http://" + "localhost" + ":" + str(each_tablet) + "/api/tablethb"
            try:
                resp = requests.get(url)
            except requests.exceptions.ConnectionError as e:
                if each_tablet not in recovered:
                    recovered.append(each_tablet)
                    present_tables = [one_tablet_name for one_tablet_name in
                                           tablet_dict if one_tablet_name != each_tablet]
                    if len(present_tables):
                        recovery_port = present_tables[0]
                        recovery_url = "http://" + "localhost" + ":" + str(recovery_port) + "/api/recover"
                        requests.get(recovery_url)
        time.sleep(5)


if __name__ == '__main__':
    lines = [line.rstrip('\n') for line in open('tablet.mk')]

    for each_l in lines:
        for each_line in each_l:
            strings = each_l.split("|")
            if strings[1] not in tablet_dict.keys():
                tablet_dict[strings[1]] = strings[0]

    t = threading.Thread(target=heartbeat)
    t.start()

    master_server.run(host=sys.argv[1], port=sys.argv[2])


