import flask
from flask import request, jsonify, Response

tablet_server = flask.Flask(__name__)
tablet_server.config["DEBUG"] = True

# Create some test data for our catalog in the form of a list of dictionaries.
books = [
    {'id': 0,
     'title': 'A Fire Upon the Deep',
     'author': 'Vernor Vinge',
     'first_sentence': 'The coldsleep itself was dreamless.',
     'year_published': '1992'},
    {'id': 1,
     'title': 'The Ones Who Walk Away From Omelas',
     'author': 'Ursula K. Le Guin',
     'first_sentence': 'With a clamor of bells that set the swallows soaring, the Festival of Summer came to the city Omelas, bright-towered by the sea.',
     'published': '1973'},
    {'id': 2,
     'title': 'Dhalgren',
     'author': 'Samuel R. Delany',
     'first_sentence': 'to wound the autumnal city.',
     'published': '1975'}
]
'tablet to server name mapping'
tablet_serv_name_mapping = dict()
'the list of tables in this tablet'
tables_list = list()
'path of persistent storage'
persistant_storage = '/Users/raghavs/14848/starter/persist/'

"""
    Method that appends a table name to the 
    list of tables for this server
"""


def create_table_self(table_name):
    path = persistant_storage + table_name + "_meta_data.mdt"
    file_desc = open(path, 'w')
    file_desc.write("Table:" + table_name + "\n")
    file_desc.close()

    write_ahead_log = persistant_storage + table_name + ".wal"
    file_desc = open(write_ahead_log, 'w')
    file_desc.write("Table:" + table_name + "\n")
    file_desc.close()
    tables_list.append(table_name)
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
    table_info = request.get_json()
    if table_info is None:
        return Response(status=400)
    for server_name in tables_list:
        print(server_name)
        if table_info['name'] == server_name:
            return Response(status=409)

    if table_info['name'] not in tables_list:
        response = create_table_self(table_info['name'])
        return response


"""
    List all tables that are present on this tablet
    :return 200 and content with the table info 
    : return 200 with empty content if not tables are defined 
"""


@tablet_server.route('/api/tables', methods=['GET'])
def list_tables():
    response = dict()
    table_names = list()
    output = dict()
    global tables_list
    for table_name in tables_list:
        table_names.append(table_name)
    response["tables"] = table_names
    output = jsonify(response)
    print(output)
    output.status_code = 200
    return output


@tablet_server.route('/api/v1/resources/books', methods=['GET'])
def api_id():
    # Check if an ID was provided as part of the URL.
    # If ID is provided, assign it to a variable.
    # If no ID is provided, display an error in the browser.
    if 'id' in request.args:
        id = int(request.args['id'])
    else:
        return "Error: No id field provided. Please specify an id."

    # Create an empty list for our results
    results = []

    # Loop through the data and match results that fit the requested ID.
    # IDs are unique, but other fields might return many results
    for book in books:
        if book['id'] == id:
            results.append(book)

    # Use the jsonify function from Flask to convert our list of
    # Python dictionaries to the JSON format.
    return jsonify(results)


# @tablet_server.route('/api/tables/:pk', methods=['DELETE'])


tablet_server.run()
