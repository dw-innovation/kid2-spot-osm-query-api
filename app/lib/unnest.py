from .utils import clean_query


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
                            unnest(osm_ids) AS osm_id,
                            setid,
                            setname
                        FROM
                            RelationalCTE
                        )"""
        # Add the UnnestedCTE to the list of CTEs
        ctes.append(unnestCTE)

        # Clean the CTEs and concatenate them into a single query
        ctes_str = ",\n".join([clean_query(cte) for cte in ctes])

        # Limit to distinct rows and join with the nodes table to get the tags and geometry
        concatenated = f"""{ctes_str}
                        SELECT DISTINCT UnnestedCTE.id, UnnestedCTE.osm_id, UnnestedCTE.setid, UnnestedCTE.setname,
                            COALESCE(nodes.tags, ways.tags, relations.tags) AS tags,
                            COALESCE(nodes.geom, ways.geom, relations.geom) AS geom
                        FROM 
                            UnnestedCTE
                        LEFT JOIN 
                            nodes ON UnnestedCTE.osm_id = nodes.node_id
                        LEFT JOIN 
                            ways ON UnnestedCTE.osm_id = ways.way_id
                        LEFT JOIN 
                            relations ON UnnestedCTE.osm_id = relations.relation_id
                        WHERE nodes.node_id IS NOT NULL OR ways.way_id IS NOT NULL OR relations.relation_id IS NOT NULL;"""

        return concatenated

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
