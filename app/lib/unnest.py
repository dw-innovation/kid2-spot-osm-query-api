from .utils import cleanQuery

def unnest(ctes):

    ctes_str = ",\n".join([cleanQuery(cte) for cte in ctes])

    concatenated = f"""{ctes_str}
    SELECT * FROM RelationalCTE;"""
    unnestedQuery = f"""{ctes_str}
                    SELECT
                        unnested.node_id,
                        nodes.tags,
                        nodes.geom
                    FROM NestedResults,
                    LATERAL unnest(NestedResults.nodes) AS unnested(node_id)
                    JOIN nodes ON unnested.node_id = nodes.node_id;"""
    return concatenated # skip unnestedQuery for now
