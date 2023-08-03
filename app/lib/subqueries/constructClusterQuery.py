def constructClusterSubquery(node):
    eps = node.get('maxDist', 50)
    minpoints = node.get('minPts', 2)
    setid = node.get('id', 'id')
    setname = node.get('n', 'name')
    clusterName = f"cluster_{setid}_{setname}"
    clusterElementsName = f"clusterElements_{setid}_{setname}"

    subquery = f"""WITH {clusterName} AS (
                SELECT
                    ST_ClusterDBSCAN(ST_Transform(geom, 3857), eps := {eps}, minpoints := {minpoints}) OVER () AS cluster_id,
                    node_id,
                    geom
                FROM nodes
                WHERE 
                    tags->>'amenity'='cafe'
                ),
                {clusterElementsName} AS (
                SELECT
                    'cluster_' || '{setid}_' || cluster_id AS id,
                    ST_Centroid(ST_Collect(geom)) AS geom,
                    ARRAY_AGG(node_id) AS nodes,
                    '{setid}' AS setid,
                    '{setname}' AS setname
                FROM {clusterName}
                WHERE cluster_id IS NOT NULL
                GROUP BY cluster_id)
                SELECT * FROM {clusterElementsName}""" 
    return subquery
