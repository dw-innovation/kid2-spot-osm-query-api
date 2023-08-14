from flask import g


class AreaFilterError(Exception):
    """Custom error for problems encountered when constructing area filters."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def construct_area_filter():
    """Function to construct area filter for the WHERE clause of the SQL query."""
    type = g.area["type"]

    # If the area type is a bounding box, polygon, or area, call the respective function
    if type == "bbox":
        return construct_bbox_filter()
    elif type == "polygon":
        return construct_polygon_filter()
    elif type == "area":
        return construct_area_geojson_filter()

    return None


def construct_bbox_filter():
    """Constructs a bounding box filter for the WHERE clause."""
    bbox = g.area["value"]
    return f"ST_Contains(ST_MakeEnvelope({bbox[0]}, {bbox[1]}, {bbox[2]}, {bbox[3]}, 4326), geom)"


def construct_polygon_filter():
    """Constructs a polygon filter for the WHERE clause."""
    polygon = g.area["value"]
    if polygon[0] != polygon[-1]:
        polygon.append(polygon[0])
    polygon_coordinates = ", ".join([f"{coord[0]} {coord[1]}" for coord in polygon])
    return (
        f"ST_Contains(ST_GeomFromText('POLYGON(({polygon_coordinates}))', 4326), geom)"
    )


def construct_area_geojson_filter():
    """Constructs a GeoJSON area filter for the WHERE clause."""
    geojson = g.area["value"]
    return f"ST_Contains(ST_GeomFromGeoJSON('{geojson}'), geom)"
