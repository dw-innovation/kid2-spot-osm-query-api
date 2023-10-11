from psycopg2 import sql
import os
from flask import g
from .construct_where_clause import construct_CTE_where_clause


def construct_NWR_CTE(node, area):
    set_id = node.get("id", "id")
    set_name = node.get("name", "name")
    filters = construct_CTE_where_clause(node.get("filters", []), area)

    query = sql.SQL(
        """
        SELECT 
            {set_id} AS set_id,
            ST_Transform(geom, {utm}) AS transformed_geom,
            geom,
            primitive_type || '/' || node_id AS osm_ids,
            {set_id} AS set_id,
            {set_name} AS set_name,
            tags,
            primitive_type
        FROM
            {table_view}
        WHERE
            {filters}
        """
    ).format(
        set_id=sql.Literal(str(set_id)),
        utm=sql.Literal(g.utm),
        set_name=sql.Literal(set_name),
        table_view=sql.Identifier(os.getenv("TABLE_VIEW")),
        filters=filters,
    )

    cte = sql.SQL("{set_id} AS ({query})").format(
        set_id=sql.Identifier(str(set_id)),
        query=query,
    )

    return cte
