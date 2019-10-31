import flask
import requests
from flask import request, jsonify, Response
import sys
import pdb
import json

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
    list of tables in use
"""
open_list = set()


def collect_tables_from_tablets():
    global tablet_table_dict
    tables_list = list()
    for each_tablet in tablet_dict.keys():
        url = "http://" + "localhost" + ":" + str(each_tablet) + "/api/tables"
        response = requests.get(url).content
        val = json.loads(response)
        list_of_tables = val['tables']
        tablet_table_dict[each_tablet] = list_of_tables
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


@master_server.route('/api/tables/<path:text>', methods=['GET'])
def get_particular_table_info(text):
    list_of_all_tables = collect_tables_from_tablets()
    if text not in list_of_all_tables:
        return Response(status=404)

    for each_tablet_key in tablet_table_dict:
        if text in tablet_table_dict[each_tablet_key]:
            connect_url = "http://" + "localhost" + ":" + str(each_tablet_key) + "/api/tables/" + text
            response = requests.get(connect_url).content
            # pdb.set_trace()
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
    length = sys.maxsize
    smallest_tablet = ""
    for each_tablet in tablet_table_dict.keys():
        this_len = len(tablet_table_dict[each_tablet])
        if this_len < length:
            smallest_tablet = each_tablet
    return smallest_tablet


def create_a_table_given_tablet(tablet_serv_key, content):
    tablet_url = "http://" + "localhost" + ":" + str(tablet_serv_key) + "/api/tables"
    response = requests.post(tablet_url, None, content)
    if response.status_code == 200:
        return True
    else:
        return False


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
    list_of_tables = collect_tables_from_tablets()
    if text not in list_of_tables:
        return Response(status=404)
    if text in open_list:
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


lines = [line.rstrip('\n') for line in open('tablet.mk')]

for each_l in lines:
    for each_line in each_l:
        strings = each_l.split("|")
        if strings[1] not in tablet_dict.keys():
            tablet_dict[strings[1]] = strings[0]

master_server.run(host=sys.argv[1], port=sys.argv[2])
