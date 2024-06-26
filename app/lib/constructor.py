from .ctes.construct import construct_ctes
from .construct_relations import construct_relations
from psycopg2 import sql
from flask import g


def construct_query_from_graph(spot_query):
    try:
        # Construct the node CTEs based on the intermediate representation
        ctes = construct_ctes(spot_query)

        # Combine the node constructed CTEs with the SQL WITH clause
        combined_ctes = sql.SQL("WITH ") + sql.SQL(", ").join(ctes)

        # Construct the relations (JOINs) based on the intermediate representation
        relations = construct_relations(spot_query)

        # Combine CTEs and relations to form the final query
        final_query = sql.SQL(" ").join([combined_ctes, relations])

        # Return the final SQL query
        return final_query

    except Exception as e:
        print(f"An error occurred in constructor.py: {e}")
        return None
