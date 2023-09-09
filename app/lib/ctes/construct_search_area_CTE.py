from flask import g
from psycopg2 import sql


class AreaInvalidError(Exception):
    pass


def construct_search_area_CTE():
    # Get the geometry and type from Flask's global object
    geom = g.area["value"]
    type = g.area["type"]

    # Handle bounding box type
    try:
        if type == "bbox":
            geometry = sql.SQL(
                "ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, {utm})"
            ).format(
                utm=sql.Literal(g.utm),
                xmin=sql.Literal(geom[0]),
                ymin=sql.Literal(geom[1]),
                xmax=sql.Literal(geom[2]),
                ymax=sql.Literal(geom[3]),
            )

        # Handle polygon type
        elif type == "polygon":
            # Make sure the polygon is closed by appending the first point at the end if needed
            if geom[0] != geom[-1]:
                geom.append(geom[0])

            # Convert list of coordinates to a string
            polygon_coordinates = ", ".join(
                [f"{coord[0]} {coord[1]}" for coord in geom]
            )

            # Use ST_GeomFromText to create the geometry
            geometry = sql.SQL("ST_GeomFromText('POLYGON(({polygon}))', {utm})").format(
                utm=sql.Literal(g.utm), polygon=sql.Literal(polygon_coordinates)
            )

        # Handle geojson area type
        elif type == "area":
            geometry = sql.SQL("ST_GeomFromGeoJSON({})").format(sql.Literal(geom))

        # Construct the CTE (Common Table Expression) using the generated geometry
        cte = sql.SQL("{envelope} AS (SELECT {geometry} AS geom)").format(
            envelope=sql.Identifier("envelope"), geometry=geometry
        )

        return cte
    except Exception as e:
        raise AreaInvalidError(e)
