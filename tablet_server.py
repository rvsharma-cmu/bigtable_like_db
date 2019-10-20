import flask
from flask import request, jsonify, Response

tablet_server = flask.Flask(__name__)
tablet_server.config["DEBUG"] = True

'tablet to server name mapping'
tablet_serv_name_mapping = dict()
'the list of tables in this tablet server'
tables_list = list()
'path of persistent storage'
persistent_storage = '/Users/raghavs/14848/starter/persist/'
'dictionary for table. It will have the properties of table ' \
'like column name and rows'
table_contents = dict()

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

        # print("I am in else")
        # if tab_name not in tables_list:
        #     return Response(status=404)
        # else:
        #     output = dict()
        #     values = table_contents[tab_name]
        #     output = jsonify(values)
        #     output.status_code = 200
        #     return output


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


@tablet_server.route('/api/table/<path:text>/cell', methods=['POST'])
def insert_a_cell(text):
    import pdb; pdb.set_trace()
    print(text)


tablet_server.run()
