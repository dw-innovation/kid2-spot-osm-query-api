import os
from flask import g
from ..utils import distance_to_meters
from .construct_where_clause import construct_cte_where_clause
from psycopg2 import sql


def construct_cluster_cte(node):
    """
    Constructs a SQL Common Table Expression (CTE) that performs spatial clustering
    using the DBSCAN algorithm on OSM features.

    This function:
      - Converts clustering parameters to meters.
      - Constructs a SQL CTE that clusters spatial features using PostGIS's
        `ST_ClusterDBSCAN`.
      - Computes centroids and collects relevant metadata for each cluster.

    Args:
        node (dict): A dictionary defining the node configuration. Expected keys:
            - "maxDistance" (str or float): Max distance for DBSCAN, defaults to "50".
            - "minPoints" (int): Minimum number of points to form a cluster, defaults to 2.
            - "id" (str): Unique ID for the cluster set.
            - "name" (str): Human-readable name of the cluster set.
            - "filters" (list): List of filter conditions to be applied.

    Returns:
        psycopg2.sql.Composed: A SQL statement defining the CTE for clustering.

    Raises:
        None explicitly, but any errors during SQL generation are caught and printed.

    Notes:
        - Uses the current UTM zone from `flask.g.utm`.
        - The resulting clusters are grouped by DBSCAN cluster ID, with geometry
          centroids and associated metadata.
        - Returns None if an exception is raised during construction.
    """
    try:
        eps = node.get("maxDistance", "50")
        eps_in_meters = distance_to_meters(eps)
        min_points = node.get("minPoints", 2)
        set_id = node.get("id", "id")
        set_name = node.get("name", "name")
        cluster_name = f"cluster_{set_id}_{set_name}".replace(" ", "_")

        print(g.utm,eps_in_meters, min_points, set_id, set_name)
        filters = construct_cte_where_clause(node.get("filters", []))

        query = sql.SQL(
            """WITH clusters AS (
                                SELECT
                                    ST_ClusterDBSCAN(ST_Transform(geom, {utm}), eps := {eps}, minpoints := {min_pts}) OVER () AS cluster_id,
                                    node_id,
                                    ST_Transform(geom, {utm}) AS transformed_geom,
                                    geom,
                                    tags,
                                    primitive_type
                                FROM {table_view}
                                WHERE
                                    {filters}
                            )
                            SELECT
                                'cluster_' || {cluster_name} || cluster_id AS id,
                                ST_Centroid(ST_Collect(geom)) AS geom,
                                ARRAY_AGG(primitive_type || '/' || node_id::text) AS osm_ids,
                                {set_id} AS set_id,
                                {set_name} AS set_name,
                                transformed_geom,
                                tags,
                                primitive_type
                            FROM clusters
                            WHERE cluster_id IS NOT NULL
                            GROUP BY cluster_id, tags, primitive_type, transformed_geom"""
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

        cte = sql.SQL("{set_id} AS ({q})").format(
            set_id=sql.Identifier(str(set_id)),
            q=query,
        )

        return cte

    except Exception as e:
        print(f"An error occurred in cluster.py: {e}")
        return None
