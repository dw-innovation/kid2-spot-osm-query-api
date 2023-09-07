import json
import re
from flask import g
import requests
from shapely import wkb
from shapely.geometry import mapping


def geom_bin_to_geojson(geom_bin):
    """
    Convert PostGIS binary geometries into GeoJSON format.
    """

    geom_wkt = wkb.loads(geom_bin, hex=True)  # Convert binary geometry to WKT
    return mapping(geom_wkt)  # Convert WKT to GeoJSON


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
        "km": 1000,
        "ft": 0.3048,
        "mile": 1609.34,
        "mi": 1609.34,
        "yd": 0.9144,
        "in": 0.0254,
        "cm": 0.01,
        "mm": 0.001,
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


def construct_primitives_CTEs(query):
    primitives = ["node", "way", "relation"]

    cte_query_parts = [
        f"""
        {primitive}s_CTE AS (
            {query.replace("[primitive]", primitive)}
        )"""
        for primitive in primitives
    ]

    concatenated_ctes = "WITH " + ", ".join(cte_query_parts)
    union_clauses = " UNION ALL ".join(
        f"SELECT * FROM {primitive}s_CTE" for primitive in primitives
    )

    cte = f"""{concatenated_ctes}{union_clauses}"""

    return cte


def set_area(data):
    type = data["a"]["t"]

    if not hasattr(g, "area"):
        g.area = {}

    if type == "bbox":
        g.area["type"] = "bbox"
        g.area["value"] = data["a"]["v"]
        minx, miny, maxx, maxy = data["a"]["v"]
        center_x = (minx + maxx) / 2
        center_y = (miny + maxy) / 2
        g.area["center"] = [center_x, center_y]
        g.utm = determine_utm_epsg(center_y, center_x)

    elif type == "polygon":
        g.area["type"] = "polygon"
        g.area["value"] = data["a"]["v"]

    elif type == "area":
        g.area["type"] = "area"
        area_name = data["a"]["v"]

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
