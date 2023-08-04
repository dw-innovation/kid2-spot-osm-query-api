from .constructCTEWhereClause import constructCTEWhereClause

def constructNWRCTE(node, area):
    setid = node.get('id', 'id')
    setname = node.get('n', 'name')
    ctename = f"nwr_{setid}_{setname}"
    filters = constructCTEWhereClause(node.get('flts', []), area)

    cte = f"""{ctename} AS (SELECT 
                    'nwr_' || '{setid}_' || node_id AS id,
                    geom,
                    ARRAY_AGG(node_id) AS nodes,
                    '{setid}' AS setid,
                    '{setname}' AS setname
                    FROM
                        nodes
                    WHERE
                        {filters}
                    GROUP BY node_id, geom)""" 
    return cte
