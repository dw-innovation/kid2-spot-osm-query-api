import json
import os
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from jsonschema import validate as validate_json_schema, exceptions
import psycopg2
from psycopg2 import DatabaseError, ProgrammingError, InterfaceError, OperationalError
from psycopg2.extensions import QueryCanceledError
from lib.ctes.construct_search_area import (
    AreaInvalidError,
)
from lib.utils import (
    get_spots,
    set_area,
    results_to_geojson,
    check_area_surface,
    validate_spot_query,
    clean_spot_query,
)
from lib.database import initialize_connection_pool, get_db, close_db
import lib.constructor as constructor
from collections import Counter
from lib.timer import Timer
from flask_compress import Compress
from dotenv import load_dotenv
import jwt
from jwt import InvalidTokenError

load_dotenv()

def check_env_vars():
    required_vars = [
        "DATABASE_NAME",
        "DATABASE_USER",
        "DATABASE_PASSWORD",
        "DATABASE_HOST",
        "DATABASE_PORT",
        "TABLE_VIEW",
        "JWT_SECRET"
    ]
    missing_vars = [var for var in required_vars if os.getenv(var) is None]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

check_env_vars()

environment = os.getenv("ENVIRONMENT") or "production"

compress = Compress()
app = Flask(__name__)
compress.init_app(app)
CORS(app)

DATABASE = {
    "name": os.getenv("DATABASE_NAME"),
    "user": os.getenv("DATABASE_USER"),
    "password": os.getenv("DATABASE_PASSWORD"),
    "host": os.getenv("DATABASE_HOST"),
    "port": os.getenv("DATABASE_PORT"),
}

JWT_SECRET = os.getenv("JWT_SECRET")

with open("./schemas/spot_query.json", "r") as file:
    schema = json.load(file)

@app.before_first_request
def setup():
    initialize_connection_pool(DATABASE)

@app.teardown_appcontext
def teardown(e=None):
    close_db(e)

def validate_jwt(token):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return payload
    except InvalidTokenError as e:
        raise ValueError(f"Invalid token: {str(e)}")

@app.before_request
def check_jwt():
    if request.endpoint not in ['static']:
        auth_header = request.headers.get("Authorization")
        if auth_header is None or not auth_header.startswith("Bearer "):
            return jsonify({"status": "error", "message": "Missing or invalid token"}), 401
        
        token = auth_header.split(" ")[1]
        try:
            validate_jwt(token)
        except ValueError as e:
            return jsonify({"status": "error", "message": str(e)}), 401

@app.route("/validate-spot-query", methods=["POST"])
def validate_spot_query_route():
    data = request.json

    try:
        validate_json_schema(data, schema)
        validate_spot_query(data)

        return jsonify({"status": "success"}), 200
    except (exceptions.ValidationError, ValueError) as e:
        return (
            jsonify(
                {"status": "error", "errorType": "spot_queryInvalid", "message": str(e)}
            ),
            400,
        )

@app.route("/get-pg-query", methods=["POST"])
def get_spot_query_route():
    data = request.json
    db = get_db()
    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        validate_json_schema(data, schema)
        validate_spot_query(data)
    except (exceptions.ValidationError, ValueError) as e:
        print(e)
        return jsonify({"error": str(e)}), 400

    try:
        cleaned_spot_query = clean_spot_query(data)
        set_area(cleaned_spot_query)
        query = constructor.construct_query_from_graph(cleaned_spot_query)

        return query.as_string(cursor)

    except AreaInvalidError as e:
        response = {
            "status": "error",
            "errorType": "areaInvalid",
            "message": str(e),
        }

        return jsonify(response), 422

    except ValueError as e:
        response = {
            "status": "error",
            "errorType": "valueError",
            "message": str(e),
        }

        return jsonify(response), 400

    except (InterfaceError, ProgrammingError, DatabaseError, OperationalError) as e:
        response = {
            "status": "error",
            "errorType": str(e),
        }

        return jsonify(response), 500

@app.route("/run-spot-query", methods=["POST"])
def run_spot_query_route():
    timer = Timer()
    data = request.json
    db = get_db()

    try:
        validate_json_schema(data, schema)
        validate_spot_query(data)

    except (exceptions.ValidationError, ValueError) as e:
        return (
            jsonify(
                {"status": "error", "errorType": "spot_queryInvalid", "message": str(e)}
            ),
            400,
        )

    try:
        cleaned_spot_query = clean_spot_query(data)
        set_area(cleaned_spot_query)
        timer.add_checkpoint("area_setting")
        check_area_surface(g.db, g.area["value"], g.area["type"], g.utm)

        query = constructor.construct_query_from_graph(cleaned_spot_query)

        timer.add_checkpoint("query_construction")

        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("SET statement_timeout = 20000")
        db.commit()
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
            "status": "success",
        }

        return jsonify(response), 200

    except AreaInvalidError as e:
        timer.add_checkpoint("error")
        response = {
            "status": "error",
            "errorType": "areaInvalid",
            "message": str(e),
            "timing": timer.get_all_checkpoints(),
        }

        return jsonify(response), 422

    except QueryCanceledError:
        timer.add_checkpoint("timeout")
        response = {
            "status": "error",
            "errorType": "queryTimeout",
            "timing": timer.get_all_checkpoints(),
        }

        return jsonify(response), 408

    except ValueError as e:
        timer.add_checkpoint("error")
        response = {
            "status": "error",
            "errorType": "valueError",
            "message": str(e),
            "timing": timer.get_all_checkpoints(),
        }

        return jsonify(response), 400

    except (InterfaceError, ProgrammingError, DatabaseError, OperationalError) as e:
        timer.add_checkpoint("error")
        response = {
            "status": "error",
            "errorType": str(e),
            "timing": timer.get_all_checkpoints(),
        }

        return jsonify(response), 500

if __name__ == "__main__":
    app.run(debug=True)
