from .constructWhereClause import constructCTEWhereClause

def constructClusterCTE(node, area):
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
        # Get distance and minpoints from node
        eps = node.get('maxDist', 50)
        minpoints = node.get('minPts', 2)

        # Create setid and setname
        setid = node.get('id', 'id')
        setname = node.get('n', 'name')

        # Create cluster name
        clusterName = f"cluster_{setid}_{setname}"

        # Create WHERE clause
        filters = constructCTEWhereClause(node.get('flts', []), area)

        cte = f"""{clusterName} AS (
                    WITH clusters AS (
                        SELECT
                            ST_ClusterDBSCAN(ST_Transform(geom, 3857), eps := {eps}, minpoints := {minpoints}) OVER () AS cluster_id,
                            node_id,
                            geom
                        FROM nodes
                        WHERE
                            {filters}
                        )
                    SELECT
                        'cluster_' || '{setid}_' || cluster_id AS id,
                        ST_Centroid(ST_Collect(geom)) AS geom,
                        ARRAY_AGG(node_id) AS nodes,
                        '{setid}' AS setid,
                        '{setname}' AS setname
                    FROM clusters
                    WHERE cluster_id IS NOT NULL
                    GROUP BY cluster_id
                    )"""
        
        return cte

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
