from .utils import cleanQuery

def unnest(ctes):
    """
    This function takes a list of common table expressions (CTEs), 
    adds an UnnestedCTE to this list, and constructs a PostgreSQL query 
    that unnests the nodes and joins the result with the nodes table.
    
    :param ctes: list of common table expressions (CTEs) to include in the query
    :type ctes: List[str]
    :return: SQL query string
    :rtype: str
    """
    
    try:
        # Construct an UnnestedCTE from the RelationalCTE
        unnestCTE = """UnnestedCTE AS (
                        SELECT
                            id,
                            unnest(nodes) AS node_id,
                            setid,
                            setname
                        FROM
                            RelationalCTE
                        )"""
        # Add the UnnestedCTE to the list of CTEs
        ctes.append(unnestCTE)

        # Clean the CTEs and concatenate them into a single query
        ctes_str = ",\n".join([cleanQuery(cte) for cte in ctes])

        # Limit to distinct rows and join with the nodes table to get the tags and geometry
        concatenated = f"""{ctes_str}
                        SELECT DISTINCT UnnestedCTE.id, UnnestedCTE.node_id, UnnestedCTE.setid, UnnestedCTE.setname, nodes.tags, nodes.geom
                        FROM 
                            UnnestedCTE
                        JOIN 
                            nodes ON UnnestedCTE.node_id = nodes.node_id;"""
        
        return concatenated

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
