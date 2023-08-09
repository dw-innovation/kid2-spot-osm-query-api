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
