from psycopg2 import sql
import os
from flask import g
from .construct_where_clause import construct_CTE_where_clause


def construct_NWR_CTE(node, area):
    set_id = node.get("id", "id")
    set_name = node.get("n", "name")
    CTE_name = f"nwr_{set_id}_{set_name}".replace(" ", "_")
    filters = construct_CTE_where_clause(node.get("flts", []), area)

    query = sql.SQL(
        """SELECT 
        'nwr_' || {set_id} || '_id' AS id,
        ST_Transform(geom, {utm}) AS transformed_geom,
        geom,
        node_id AS osm_ids,
        {set_id} AS set_id,
        {set_name} AS set_name,
        tags
    FROM
        {table_view}
    WHERE
        {filters}"""
    ).format(
        set_id=sql.Literal(set_id),
        utm=sql.Literal(g.utm),
        set_name=sql.Literal(set_name),
        table_view=sql.Identifier(os.getenv("TABLE_VIEW")),
        filters=filters,
    )

    cte = sql.SQL("{CTE_name} AS ({query})").format(
        CTE_name=sql.Identifier(CTE_name),
        query=query,
    )

    return cte
