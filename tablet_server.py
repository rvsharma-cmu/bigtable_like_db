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
tablet_serv_name_mapping = dict()
tables_list = list()
persist_storage = '/Users/raghavs/14848/starter/persist/'


def create_table_self(table_name):
    result = dict()
    path = persist_storage + table_name + "_meta_data.txt"
    file_desc = open(path, 'w')
    file_desc.write("Table:" + table_name + "\n")
    file_desc.close()

    write_ahead_log = persist_storage + table_name + ".wal"
    file_desc = open(path, 'w')
    file_desc.write("Table:" + table_name + "\n")
    file_desc.close()
    # result['success'] = 1
    # result['message'] = "Table Created"
    # response = jsonify()
    # response
    # response.status_code = 200
    # response.content = 0
    # resp = make_response(render_template('error.html'), 404)
    # resp.headers['X-Something'] = 'A value'
    return Response(status=200)


@tablet_server.route('/api/tables', methods=['POST'])
def create_table():
    # create a table with name
    result = dict()
    table_info = request.get_json()
    for server_name in tables_list:
        if table_info['name'] == tables_list:
            # result['message'] = "Failure - Table exists."
            # response = jsonify()
            # response.status_code = 409
            return Response(status=409)

    if not tables_list:
        response = create_table_self(table_info['name'])
        return response


@tablet_server.route('/api/tables/', methods=['GET'])
def api_all():
    return jsonify(books)


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
