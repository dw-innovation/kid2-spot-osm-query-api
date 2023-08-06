from .constructCTEWhereClause import constructCTEWhereClause

def constructNWRCTE(node, area):
    """
    Function to construct a Common Table Expression (CTE) for a SQL query 
    based on a provided node and area.

    :param node: A dictionary containing parameters for CTE construction.
    :type node: dict
    :param area: A parameter used in the WHERE clause for area filtering.
    :type area: str, int or float
    :return: A string representing the CTE part of a SQL query.
    :rtype: str
    """

    # Extract setid and setname from the node, default to 'id' and 'name' if not provided
    setid = node.get('id', 'id')
    setname = node.get('n', 'name')

    # Construct the CTE name
    ctename = f"nwr_{setid}_{setname}"

    # Create the WHERE clause of the SQL query
    filters = constructCTEWhereClause(node.get('flts', []), area)

    # Construct the CTE
    cte = f"""{ctename} AS (
                SELECT 
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
