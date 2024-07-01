from psycopg2 import sql
from flask import g


class AreaInvalidError(Exception):
    pass


def construct_search_area_cte(type, value):
    # Handle bounding box type
    try:
        if type == "bbox":
            geometry = sql.SQL(
                "ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, {utm})"
            ).format(
                utm=sql.Literal(g.utm),
                xmin=sql.Literal(value[0]),
                ymin=sql.Literal(value[1]),
                xmax=sql.Literal(value[2]),
                ymax=sql.Literal(value[3]),
            )

        # Handle geojson area type
        elif type == "area":
            geometry = sql.SQL("ST_GeomFromGeoJSON({value})").format(
                value=sql.Literal(value)
            )

        # Construct the CTE (Common Table Expression) using the generated geometry
        cte = sql.SQL("{envelope} AS (SELECT {geometry} AS geom)").format(
            envelope=sql.Identifier("envelope"), geometry=geometry
        )

        return cte
    except Exception as e:
        raise AreaInvalidError(e)
