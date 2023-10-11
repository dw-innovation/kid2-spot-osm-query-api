import os
from flask import g
from ..utils import distance_to_meters
from .construct_where_clause import construct_CTE_where_clause
from psycopg2 import sql


def construct_cluster_CTE(node, area):
    try:
        eps = node.get("maxDistance", "50")
        eps_in_meters = distance_to_meters(eps)
        min_points = node.get("minPoints", 2)
        set_id = node.get("id", "id")
        set_name = node.get("name", "name")
        cluster_name = f"cluster_{set_id}_{set_name}".replace(" ", "_")
        filters = construct_CTE_where_clause(node.get("filters", []), area)

        query = sql.SQL(
            """WITH clusters AS (
                                SELECT
                                    ST_ClusterDBSCAN(ST_Transform(geom, {utm}), eps := {eps}, minpoints := {min_pts}) OVER () AS cluster_id,
                                    nodes_id,
                                    geom
                                FROM {table_view}
                                WHERE
                                    {filters}
                            )
                            SELECT
                                'cluster_' || {cluster_name} || cluster_id AS id,
                                ST_Centroid(ST_Collect(geom)) AS geom,
                                ARRAY_AGG(node_id) AS osm_ids,
                                {set_id} AS set_id,
                                {set_name} AS set_name
                            FROM clusters
                            WHERE cluster_id IS NOT NULL
                            GROUP BY cluster_id"""
        ).format(
            utm=sql.Literal(g.utm),
            eps=sql.Literal(eps_in_meters),
            min_pts=sql.Literal(min_points),
            cluster_name=sql.Literal(cluster_name),
            table_view=sql.Identifier(os.getenv("TABLE_VIEW")),
            filters=filters,
            set_id=sql.Literal(set_id),
            set_name=sql.Literal(set_name),
        )

        cte = sql.SQL("{cluster_name} AS ({query})").format(
            cluster_name=sql.Identifier(cluster_name),
            query=sql.SQL(query),
        )

        return cte

    except Exception as e:
        print(f"An error occurred in cluster.py: {e}")
        return None
