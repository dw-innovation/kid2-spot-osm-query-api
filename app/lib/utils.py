import re
from shapely import wkb
from shapely.geometry import mapping


def clean_query(query):
    cleaned_query = "\n".join(line.strip() for line in query.split("\n"))
    return cleaned_query


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

        # Remove 'id' key from the result before assigning to properties since it's not needed in the properties
        if "id" in result:
            result["osm_id"] = result["node_id"]

        del result["node_id"]
        del result["id"]

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
