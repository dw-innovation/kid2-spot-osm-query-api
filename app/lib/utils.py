import json
import re
from flask import g
import requests
from shapely import wkb
from shapely.wkb import loads as wkb_loads
from shapely.geometry import (
    MultiPolygon,
    Polygon,
    MultiPoint,
    LineString,
    Point,
    mapping,
    shape,
)
from math import cos, radians
from psycopg2 import sql
from collections import defaultdict
import os


from lib.ctes.construct_search_area_CTE import AreaInvalidError


def geom_bin_to_geojson(geom_bin):
    """
    Convert PostGIS binary geometries into GeoJSON format.
    """

    geom_wkt = wkb.loads(geom_bin, hex=True)  # Convert binary geometry to WKT
    return mapping(geom_wkt)  # Convert WKT to GeoJSON


def add_center_to_geojson(geojson_feature):
    """
    Add a center point to a GeoJSON feature.
    """
    geometry_shape = shape(geojson_feature["geometry"])
    centroid = geometry_shape.centroid
    center_point = Point(centroid.x, centroid.y).__geo_interface__
    geojson_feature["properties"]["center"] = center_point


def results_to_geojson(results):
    """
    Convert results list to GeoJSON.
    """

    features = []

    for result in results:
        geom_geojson = geom_bin_to_geojson(
            result["geom"]
        )  # Convert geometry to GeoJSON

        # Remove 'geom' key from the result before assigning to properties since it's not needed in the properties
        del result["geom"]

        feature = {
            "type": "Feature",
            "geometry": geom_geojson,
            "properties": result,
        }

        add_center_to_geojson(feature)  # Add center point to the feature

        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    return geojson


def distance_to_meters(distance_str):
    """
    Convert distance with unit to meters.
    """

    # Conversion rates for different units
    conversion_rates = {
        "m": 1,
        "meters": 1,
        "meter": 1,
        "metres": 1,
        "km": 1000,
        "kilometer": 1000,
        "kilometers": 1000,
        "ft": 0.3048,
        "foot": 0.3048,
        "mile": 1609.34,
        "miles": 1609.34,
        "mi": 1609.34,
        "yd": 0.9144,
        "yard": 0.9144,
        "yards": 0.9144,
        "in": 0.0254,
        "inch": 0.0254,
        "inches": 0.0254,
        "cm": 0.01,
        "centimeter": 0.01,
        "centimeters": 0.01,
        "mm": 0.001,
        "millimeter": 0.001,
        "millimeters": 0.001,
    }

    # Use regex to extract value and unit
    match = re.match(r"(?P<value>[\d.]+)\s?(?P<unit>[a-zA-Z]*)", distance_str)

    if not match:
        raise ValueError(f"Invalid format: {distance_str}")

    value_str = match.group("value")
    unit = match.group("unit").lower()

    # If there's no unit, return the numeric value as a string
    if not unit:
        return str(float(value_str))

    # Checking if unit is valid
    if unit not in conversion_rates:
        raise ValueError(f"Unknown distance unit: {unit}")

    value = float(value_str)

    ## Converting to meters
    distance_meters = str(value * conversion_rates[unit])

    return distance_meters


def determine_utm_epsg(latitude, longitude):
    """
    Determine UTM zone EPSG for a given latitude and longitude.
    :param latitude: float
    :param longitude: float
    :return: int, UTM EPSG
    """
    if -180 <= longitude <= 180:
        zone_number = int((longitude + 180) / 6) + 1
        if -80 <= latitude < 0:
            return 32700 + zone_number  # Southern Hemisphere
        elif 0 <= latitude <= 84:
            return 32600 + zone_number  # Northern Hemisphere
        else:
            raise ValueError(
                "Latitude out of range. UTM only supports between 80°S and 84°N."
            )
    else:
        raise ValueError("Longitude out of range.")


def set_area(data):
    try:
        type = data["area"]["type"]

        if not hasattr(g, "area"):
            g.area = {}

        if type == "bbox":
            g.area["type"] = "bbox"
            g.area["value"] = data["area"]["value"]
            minx, miny, maxx, maxy = data["area"]["value"]
            center_x = (minx + maxx) / 2
            center_y = (miny + maxy) / 2
            g.area["center"] = [center_x, center_y]
            g.utm = determine_utm_epsg(center_y, center_x)

        elif type == "polygon":
            g.area["type"] = "polygon"
            g.area["value"] = data["area"]["value"]

        elif type == "area":
            g.area["type"] = "area"
            area_name = data["area"]["value"]

            # Get area name from OSM Nominatim
            url = f"https://nominatim.openstreetmap.org/search?q={area_name}&format=json&polygon_geojson=1&limit=1"
            response = requests.get(url)

            if response.status_code == 200:
                nominatim_data = json.loads(response.text)

                if len(nominatim_data) > 0 and "geojson" in nominatim_data[0]:
                    geojson = json.dumps(nominatim_data[0]["geojson"])
                    g.area["value"] = geojson
                    g.area["center"] = [
                        float(nominatim_data[0]["lat"]),
                        float(nominatim_data[0]["lon"]),
                    ]
                    g.utm = determine_utm_epsg(g.area["center"][0], g.area["center"][1])
    except Exception as e:
        print(f"An error occurred in area.py: {e}")
        raise AreaInvalidError(e)


def check_area_surface(db, geom, geom_type, utm):
    cursor = db.cursor()

    if geom_type == "bbox":
        query = sql.SQL(
            "SELECT ST_Area(ST_Transform(ST_MakeEnvelope({}, {}, {}, {}, 4326), {}))"
        ).format(
            sql.Literal(geom[0]),
            sql.Literal(geom[1]),
            sql.Literal(geom[2]),
            sql.Literal(geom[3]),
            sql.Literal(utm),
        )

    elif geom_type == "polygon":
        polygon_coordinates = ", ".join([f"{coord[0]} {coord[1]}" for coord in geom])
        query = sql.SQL(
            "SELECT ST_Area(ST_Transform(ST_GeomFromText('POLYGON(({}))', 4326), {}))"
        ).format(sql.Literal(polygon_coordinates), sql.Literal(utm), sql.Literal(utm))

    elif geom_type == "area":
        query = sql.SQL(
            "SELECT ST_Area(ST_Transform(ST_GeomFromGeoJSON({}), {}))"
        ).format(sql.Literal(geom), sql.Literal(utm))

    cursor.execute(query)
    area = cursor.fetchone()[0]

    area = area / 1e6

    max_area = int(os.getenv("MAX_AREA", 5000))

    if area > max_area:
        raise AreaInvalidError("areaExceedsLimit")


def get_spots(results):
    grouped = defaultdict(lambda: {"coords": [], "tags": None, "nodes": []})
    spots = []

    for record in results:
        primary_osm_id = record["primary_osm_ids"]  # Assuming this is an array

        for primary_osm_id in primary_osm_id:
            if record["osm_ids"] == primary_osm_id:
                grouped[primary_osm_id]["tags"] = record["tags"]

            grouped[primary_osm_id]["nodes"].append(record["osm_ids"])

            geom = wkb_loads(record["geom"], hex=False)
            coords = []

            if isinstance(geom, Point):
                coords = [geom.coords[0]]
            elif isinstance(geom, LineString):
                coords = list(geom.coords)
            elif isinstance(geom, Polygon):
                coords = list(geom.exterior.coords)
            elif isinstance(geom, MultiPolygon):
                for (
                    polygon
                ) in (
                    geom.geoms
                ):  # Corrected iteration over each Polygon in a MultiPolygon
                    coords.extend(list(polygon.exterior.coords))

            else:
                pass

            grouped[primary_osm_id]["coords"].extend(coords)

    buffer_meters = 100
    buffer_lat = buffer_meters / 111000.0

    for primary_osm_id, data in grouped.items():
        multi_point = MultiPoint(data["coords"])
        minx, miny, maxx, maxy = multi_point.bounds
        buffer_lon = buffer_meters / (111000.0 * cos(radians(miny)))

        minx -= buffer_lon
        miny -= buffer_lat
        maxx += buffer_lon
        maxy += buffer_lat

        bbox = [minx, miny, maxx, maxy]
        tags = data["tags"]
        nodes = list(set(data["nodes"]))  # Remove duplicates

        spots.append({"bbox": bbox, "id": primary_osm_id, "tags": tags, "nodes": nodes})

    return spots


def validate_imr(imr):
    edges, nodes = imr.get("edges"), imr.get("nodes")

    for node in nodes:
        filters = node.get("filters")
        if not validate_has_filter(filters):
            raise ValueError(f"missingFilterInNode")

    if nodes is None:
        raise ValueError("missingNodes")

    node_ids = {node["id"] for node in nodes}
    node_names = {node["name"] for node in nodes}
    seen_edges = set()

    if len(node_ids) != len(nodes):
        raise ValueError("duplicateNodeIds")

    if len(node_names) != len(nodes):
        raise ValueError("duplicateNodeNames")

    if edges is None:
        return

    for edge in edges:
        source, target = edge.get("source"), edge.get("target")

        edge_tuple = (min(source, target), max(source, target))
        if edge_tuple in seen_edges:
            raise ValueError("duplicateOrInvertedEdge")

        seen_edges.add(edge_tuple)

        if None in [source, target]:
            raise ValueError("missingEdgeSourceOrTarget")

        if source == target:
            raise ValueError("selfReferencingEdge")

        if not {source, target}.issubset(node_ids):
            raise ValueError("edgeSourceOrTargetNotInNodes")


def validate_has_filter(filter_nodes):
    if not filter_nodes:
        return False

    for filter_node in filter_nodes:
        if "key" in filter_node:
            return True
        elif "and" in filter_node:
            if validate_has_filter(filter_node["and"]):
                return True
        elif "or" in filter_node:
            if validate_has_filter(filter_node["or"]):
                return True
    return False


def fix_imr(imr):
    nodes, edges = imr.get("nodes", []), imr.get("edges", [])

    # Sort nodes by 'id'
    sorted_nodes = sorted(nodes, key=lambda x: x["id"])
    imr["nodes"] = sorted_nodes

    # Invert 'source' and 'target' where 'target' is not greater than 'source' and sort edges
    for edge in edges:
        source, target = edge.get("source"), edge.get("target")
        if source >= target:
            edge["source"], edge["target"] = target, source

    sorted_edges = sorted(edges, key=lambda x: (x["source"], x["target"]))
    imr["edges"] = sorted_edges

    return imr
