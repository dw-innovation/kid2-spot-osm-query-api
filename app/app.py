import lib.constructor as constructor
from flask import Flask, request, jsonify
from flask_cors import CORS
from jsonschema import validate, exceptions
import json
from sql_formatter.core import format_sql

app = Flask(__name__)
CORS(app)

with open('./schemas/intermediateRepresentation.json', 'r') as file:
    schema = json.load(file)

@app.route('/constructQuery', methods=['POST'])
def constructQueryRoute():
    data = request.json

    try:
        validate(data, schema)
    except exceptions.ValidationError as e:
        return jsonify({"error": str(e)}), 400

    query = constructor.constructQuery(data)
    query = query.replace("\n", " ")

    response = {
        "inputRepresentation": data,
        "generatedQuery": query
    }
    return jsonify(response), 200

if __name__ == "__main__":
    app.run(debug=True)
