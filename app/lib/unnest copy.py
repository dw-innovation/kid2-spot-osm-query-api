from .utils import cleanQuery

def unnest(subqueries):
    union_subqueries = " UNION ALL ".join([cleanQuery(subquery) for subquery in subqueries])
    unnestedQuery = f"""WITH NestedResults AS (
                        {union_subqueries}
                    )
                    SELECT
                        unnested.node_id,
                        nodes.tags,
                        nodes.geom
                    FROM NestedResults,
                    LATERAL unnest(NestedResults.nodes) AS unnested(node_id)
                    JOIN nodes ON unnested.node_id = nodes.node_id;"""
    return unnestedQuery
