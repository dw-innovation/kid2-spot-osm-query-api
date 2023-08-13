from lib.utils import construct_primitives_CTEs
from .construct_where_clause import construct_CTE_where_clause


def construct_NWR_CTE(node, area):
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
    set_id = node.get("id", "id")
    set_name = node.get("n", "name")

    # Construct the CTE name
    CTE_name = f"nwr_{set_id}_{set_name}".replace(" ", "_")

    # Create the WHERE clause of the SQL query
    filters = construct_CTE_where_clause(node.get("flts", []), area)

    query = f"""SELECT 
                    'nwr_' || '{set_id}_' || [primitive]_id AS id,
                    geom,
                    ARRAY_AGG([primitive]_id) AS osm_ids,
                    '{set_id}' AS setid,
                    '{set_name}' AS setname
                FROM
                    [primitive]s
                WHERE
                    {filters}
                GROUP BY [primitive]_id, geom"""

    primitives_cte = construct_primitives_CTEs(query)

    # Construct the CTE
    cte = f"""{CTE_name} AS ({primitives_cte})"""

    return cte
