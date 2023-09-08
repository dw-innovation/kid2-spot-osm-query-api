import json
import os
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from jsonschema import validate, exceptions
import psycopg2
from lib.utils import get_spots, set_area, results_to_geojson
from lib.database import initialize_connection_pool, get_db, close_db
import lib.constructor as constructor
from collections import Counter
from lib.timer import Timer

environment = os.getenv("ENVIRONMENT") or "production"

app = Flask(__name__)
CORS(app)

DATABASE = {
    "name": os.getenv("DATABASE_NAME"),
    "user": os.getenv("DATABASE_USER"),
    "password": os.getenv("DATABASE_PASSWORD"),
    "host": os.getenv("DATABASE_HOST"),
    "port": os.getenv("DATABASE_PORT"),
}

with open("./schemas/intermediate_representation.json", "r") as file:
    schema = json.load(file)


@app.before_first_request
def setup():
    initialize_connection_pool(DATABASE)


@app.teardown_appcontext
def teardown(e=None):
    close_db(e)


@app.route("/get-osm-query", methods=["POST"])
def get_osm_query():
    data = request.json
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        validate(data, schema)
    except exceptions.ValidationError as e:
        print(e)
        return jsonify({"error": str(e)}), 400

    try:
        set_area(data)
        query = constructor.construct_query_from_graph(data)
        return query.as_string(cursor)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/run-osm-query", methods=["POST"])
def run_osm_query():
    timer = Timer()
    data = request.json
    db = get_db()

    try:
        validate(data, schema)
    except exceptions.ValidationError as e:
        return jsonify({"error": str(e)}), 400

    try:
        set_area(data)
        timer.add_checkpoint("area_setting")

        query = constructor.construct_query_from_graph(data)

        timer.add_checkpoint("query_construction")

        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(query)
        timer.add_checkpoint("query_execution")

        # Fetch all results as a list of dictionaries
        results = [dict(record) for record in cursor]

        spots = get_spots(results)
        geojson = results_to_geojson(results)
        timer.add_checkpoint("results_transformation_to_geojson")

        distinct_set_names = list({result["set_name"] for result in results})
        set_name_counts = dict(Counter(result["set_name"] for result in results))
        area_value = getattr(g, "area", None)

        response = {
            "results": geojson,
            **(
                {"query": query.as_string(cursor)}
                if environment == "development"
                else {}
            ),
            **({"area": area_value} if area_value is not None else {}),
            "sets": {"distinct_sets": distinct_set_names, "stats": set_name_counts},
            "spots": spots,
            "timing": timer.get_all_checkpoints(),
        }

        return jsonify(response), 200

    except Exception as e:
        print(e)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
