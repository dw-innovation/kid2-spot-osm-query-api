import json
import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from jsonschema import validate, exceptions
import psycopg2
from lib.utils import resultsToGeoJSON
from lib.database import initialize_connection_pool, get_db, close_db
import lib.constructor as constructor
from collections import Counter

environment = os.getenv("ENVIRONMENT") or "production"

app = Flask(__name__)
CORS(app)

DATABASE = {
    'name': os.getenv("DATABASE_NAME"),
    'user': os.getenv("DATABASE_USER"),
    'password': os.getenv("DATABASE_PASSWORD"),
    'host': os.getenv("DATABASE_HOST"),
    'port': os.getenv("DATABASE_PORT")
}

with open('./schemas/intermediateRepresentation.json', 'r') as file:
    schema = json.load(file)

@app.before_first_request
def setup():
    initialize_connection_pool(DATABASE)

@app.teardown_appcontext
def teardown(e=None):
    close_db(e)

@app.route('/run-osm-query', methods=['POST'])
def runOSMQuery():
    data = request.json
    db = get_db()

    try:
        validate(data, schema)
    except exceptions.ValidationError as e:
        return jsonify({"error": str(e)}), 400

    try:
        query = constructor.constructQueryFromGraph(data)
        query = query.replace("\n", " ")
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(query)

        # Fetch all results as a list of dictionaries
        results = [dict(record) for record in cursor]

        geojson = resultsToGeoJSON(results)
        distinct_setnames = list({result['setname'] for result in results})
        setname_counts = dict(Counter(result['setname'] for result in results))

        if environment == "development":
            return jsonify({'geojson': geojson, 'sets': {'distinctSets': distinct_setnames, 'stats': setname_counts, 'query': query}}), 200
        else:
            return jsonify({'geojson': geojson, 'sets': {'distinctSets': distinct_setnames, 'stats': setname_counts}}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
