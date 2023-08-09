from .ctes.construct import construct_ctes
from .unnest import unnest
from .utils import clean_query
from .ctes.construct_relational_CTE import construct_relational_CTEs


def construct_query_from_graph(intermediate_representation):
    """
    This function takes a JSON representation of the intermediate query,
    constructs common table expressions (CTEs), unnests the CTEs to create individual rows,
    and cleans the resulting query.

    :param intermediate_representation: JSON representation of the intermediate query
    :type intermediate_representation: JSON object
    :return: SQL query string
    :rtype: str
    """

    try:
        # Build all CTEs, so subqueries that we can then reference in the relational CTE
        ctes = construct_ctes(intermediate_representation)

        # Build the relational CTEs
        relationalctes = construct_relational_CTEs(intermediate_representation)

        # Add the relational CTEs to the list of CTEs
        ctes.append(relationalctes)

        # Unnest the CTEs to unfold the array of nodes stored in the nodes column into individual rows
        unnestedQuery = unnest(ctes)

        # Clean the query to remove unnecessary whitespace
        cleanedQuery = clean_query(unnestedQuery)

        return cleanedQuery
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
