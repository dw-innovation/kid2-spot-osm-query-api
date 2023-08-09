import json
import requests
from flask import g


class AreaFilterError(Exception):
    """Custom error for problems encountered when constructing area filters."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def construct_area_filter(area):
    """Function to construct area filter for the WHERE clause of the SQL query."""

    if not area:
        return None

    type = area.get("t", None)

    # If the area type is a bounding box, polygon, or area, call the respective function
    if type == "bbox":
        return construct_bbox_filter(area.get("bbox", None))
    elif type == "polygon":
        return construct_polygon_filter(area.get("plygn", None))
    elif type == "area":
        return construct_area_geojson_filter(area.get("n", None))

    return None


def construct_bbox_filter(bbox):
    """Constructs a bounding box filter for the WHERE clause."""

    if bbox:
        return f"ST_Contains(ST_MakeEnvelope({bbox[0]}, {bbox[1]}, {bbox[2]}, {bbox[3]}, 4326), geom)"
    return ""


def construct_polygon_filter(polygon):
    """Constructs a polygon filter for the WHERE clause."""

    if polygon:
        if polygon[0] != polygon[-1]:
            polygon.append(polygon[0])
        polygon_coordinates = ", ".join([f"{coord[0]} {coord[1]}" for coord in polygon])
        return f"ST_Contains(ST_GeomFromText('POLYGON(({polygon_coordinates}))', 4326), geom)"
    return ""


def construct_area_geojson_filter(area_name):
    """Constructs a GeoJSON area filter for the WHERE clause."""

    if area_name:
        url = f"https://nominatim.openstreetmap.org/search?q={area_name}&format=json&polygon_geojson=1&limit=1"
        response = requests.get(url)

        if response.status_code == 200:
            data = json.loads(response.text)
            if len(data) > 0 and "geojson" in data[0]:
                geojson = json.dumps(data[0]["geojson"])

                g.area = {
                    "name": data[0]["display_name"],
                    "center": [float(data[0]["lat"]), float(data[0]["lon"])],
                    "bbox": [float(coord) for coord in data[0]["boundingbox"]],
                    "osm_id": data[0]["osm_id"],
                }

                return f"ST_Contains(ST_GeomFromGeoJSON('{geojson}'), geom)"
            else:
                raise AreaFilterError(f"No geojson data found for area '{area_name}'.")

        else:
            raise AreaFilterError(
                f"Error fetching data from Nominatim API for area '{area_name}'."
            )