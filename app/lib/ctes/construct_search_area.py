from psycopg2 import sql
from flask import g


class AreaInvalidError(Exception):
    pass


def construct_search_area_cte(type):
    # Handle bounding box type
    try:
        if type == "bbox":
            geometry = sql.SQL(
                "ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, {utm})"
            ).format(
                utm=sql.Literal(g.utm),
                xmin=sql.Literal(g.area["bbox"][0]),
                ymin=sql.Literal(g.area["bbox"][1]),
                xmax=sql.Literal(g.area["bbox"][2]),
                ymax=sql.Literal(g.area["bbox"][3]),
            )

        # Handle geojson area type
        elif type == "area":
            geometry = sql.SQL("ST_SIMPLIFY(ST_GeomFromGeoJSON({searchAreaGeometry}), 0.001)").format(
                searchAreaGeometry=sql.Literal(g.area["geometry"]),
            )

        # Construct the CTE (Common Table Expression) using the generated geometry
        cte = sql.SQL("{envelope} AS (SELECT {geometry} AS geom)").format(
            envelope=sql.Identifier("envelope"), geometry=geometry
        )

        return cte
    except Exception as e:
        raise AreaInvalidError(e)
