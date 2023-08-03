import lib.constructor as constructor
from flask import Flask, request, jsonify
from flask_cors import CORS
import jsonschema
from jsonschema import validate
import json

app = Flask(__name__)
CORS(app)

with open('./schemas/intermediateRepresentation.json', 'r') as file:
    schema = json.load(file)

@app.route('/constructQuery', methods=['POST'])
def constructQueryRoute():
    data = request.json

    try:
        validate(data, schema)
    except jsonschema.exceptions.ValidationError as e:
        return jsonify({"error": str(e)}), 400

    query = constructor.constructQuery(data)
    return query

if __name__ == "__main__":
    app.run(debug=True)
