from .ctes.construct import construct_ctes
from .unnest import unnest
from .utils import clean_query
from .ctes.construct_relational_CTE import construct_relational_CTEs
from psycopg2 import sql
from flask import g


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

        # join the relational CTEs to the list of CTEs (both are SQL composables)
        all_ctes = ctes + [relationalctes]

        final_query = sql.SQL("WITH ") + sql.SQL(", ").join(all_ctes)

        final_query += sql.SQL("SELECT * FROM Relations;")

        return final_query

    except Exception as e:
        print(f"An error occurred in constructor.py: {e}")
        return None
