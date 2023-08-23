from flask import g


def construct_envelope_CTE():
    geom = g.area["value"]
    type = g.area["type"]

    if type == "bbox":
        geometry = f"ST_MakeEnvelope({geom[0]}, {geom[1]}, {geom[2]}, {geom[3]}, 4326)"
    elif type == "polygon":
        if geom[0] != geom[-1]:
            geom.append(geom[0])
        polygon_coordinates = ", ".join([f"{coord[0]} {coord[1]}" for coord in geom])
        geometry = f"ST_GeomFromText('POLYGON(({polygon_coordinates}))', 4326)"
    elif type == "area":
        geojson = g.area["value"]
        geometry = f"ST_GeomFromGeoJSON('{geojson}')"

    cte = f"""envelope AS (SELECT {geometry} AS geom)"""

    return cte
