from ..utils import distance_to_meters
from .construct_where_clause import construct_CTE_where_clause


def construct_cluster_CTE(node, area):
    """
    This function constructs a common table expression (CTE) for clustering based on the given node and area.

    :param node: A dictionary containing cluster parameters.
    :type node: dict
    :param area: A parameter for the WHERE clause.
    :type area: str, int or float
    :return: A SQL query string representing a CTE for clustering.
    :rtype: str

    """
    try:
        # Get distance and minpoints from node and convert to meters
        eps = node.get("maxDist", "50")
        eps_in_meters = distance_to_meters(eps)

        min_points = node.get("minPts", 2)

        # Create setid and setname
        set_id = node.get("id", "id")
        set_name = node.get("n", "name")

        # Create cluster name
        cluster_name = f"cluster_{set_id}_{set_name}".replace(" ", "_")

        # Create WHERE clause
        filters = construct_CTE_where_clause(node.get("flts", []), area)

        cte = f"""{cluster_name} AS (
                    WITH clusters AS (
                        SELECT
                            ST_ClusterDBSCAN(ST_Transform(geom, 3857), eps := {eps_in_meters}, minpoints := {min_points}) OVER () AS cluster_id,
                            node_id,
                            geom
                        FROM nodes
                        WHERE
                            {filters}
                        )
                    SELECT
                        'cluster_' || '{set_id}_' || cluster_id AS id,
                        ST_Centroid(ST_Collect(geom)) AS geom,
                        ARRAY_AGG(node_id) AS nodes,
                        '{set_id}' AS setid,
                        '{set_name}' AS setname
                    FROM clusters
                    WHERE cluster_id IS NOT NULL
                    GROUP BY cluster_id
                    )"""

        return cte

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
