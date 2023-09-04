from flask import g
from ..utils import distance_to_meters
from psycopg2 import sql


def construct_relational_CTEs(imr):
    # Extract nodes and edges from the input intermediate representation
    nodes = imr["ns"]
    edges = imr["es"]

    # Construct relations for each edge
    relations = [construct_relation(edge, nodes) for edge in edges]

    # Compose the queries for each relation
    composed_queries = [relation["query"] for relation in relations]

    # Join the composed queries with commas
    composed_relations = sql.SQL(", ").join(composed_queries)

    # Create the UNION ALL clauses for the relations
    union_clauses = sql.SQL(" UNION ALL ").join(
        sql.SQL("SELECT * FROM {}").format(sql.Identifier(relation["ctename"]))
        for relation in relations
    )

    # Assemble the final CTE
    relationalCTEs = sql.SQL(
        "WITH {composed_relations} SELECT * FROM ({union_clauses}) AS Combined"
    ).format(composed_relations=composed_relations, union_clauses=union_clauses)

    final_CTE = sql.SQL("Relations AS ({})").format(relationalCTEs)

    return final_CTE


# Function to construct an individual relation
def construct_relation(edge, nodes):
    # Extract type, source_id and target_id from the input edge
    type = edge["t"]
    source_id = edge["src"]
    target_id = edge["tgt"]

    # Initialize an empty string to construct the relation query
    relational_query = ""

    # Check if the edge type is "dist"
    if type == "dist":
        # Create relation and source/target CTE names based on source_id and target_id
        relation_CTE_name = f"Dist_{source_id}_{target_id}"
        dist = edge["dist"]
        dist_in_meters = distance_to_meters(dist)

        # Find the source node and its type from the nodes list
        src_type = next((item for item in nodes if item["id"] == source_id), None)["t"]
        src_set = next((item for item in nodes if item["id"] == source_id), None)
        src_CTE_name = f"{src_type}_{src_set['id']}_{src_set['n']}".replace(" ", "_")

        # Find the target node and its type from the nodes list
        tgt_type = next((item for item in nodes if item["id"] == target_id), None)["t"]
        tgt_set = next((item for item in nodes if item["id"] == target_id), None)
        tgt_CTE_name = f"{tgt_type}_{tgt_set['id']}_{tgt_set['n']}".replace(" ", "_")

        # Construct the relation query using the defined CTE names and distance
        relational_query = sql.SQL(
            """{relation_CTE_name} AS (
                    WITH setNodes AS (
                        SELECT *, 'src' AS origin FROM {src_CTE_name}
                        UNION ALL
                        SELECT *, 'tgt' AS origin FROM {tgt_CTE_name}
                    )
                    SELECT c1.*
                    FROM setNodes AS c1
                    JOIN setNodes AS c2 ON ST_DWithin(c1.transformed_geom, c2.transformed_geom, {dist_in_meters})
                                        AND c1.set_id <> c2.set_id
                )"""
        ).format(
            relation_CTE_name=sql.Identifier(relation_CTE_name),
            src_CTE_name=sql.Identifier(src_CTE_name),
            tgt_CTE_name=sql.Identifier(tgt_CTE_name),
            utm=sql.Literal(g.utm),
            dist_in_meters=sql.Literal(dist_in_meters),
        )

    # Check if the edge type is "cnt"
    if type == "cnt":
        # Create relation and source/target CTE names based on source_id and target_id
        relation_CTE_name = f"In_{source_id}_{target_id}"

        # Find the source node and its type from the nodes list
        src_type = next((item for item in nodes if item["id"] == source_id), None)["t"]
        src_set = next((item for item in nodes if item["id"] == source_id), None)
        src_CTE_name = f"{src_type}_{src_set['id']}_{src_set['n']}".replace(" ", "_")

        # Find the target node and its type from the nodes list
        tgt_type = next((item for item in nodes if item["id"] == target_id), None)["t"]
        tgt_set = next((item for item in nodes if item["id"] == target_id), None)
        tgt_CTE_name = f"{tgt_type}_{tgt_set['id']}_{tgt_set['n']}".replace(" ", "_")

        relational_query = sql.SQL(
            """
                {relation_CTE_name} AS (
                    WITH setNodes AS (
                        SELECT *, 'src' AS origin FROM {src_CTE_name}
                        UNION ALL
                        SELECT *, 'tgt' AS origin FROM {tgt_CTE_name}
                    ),
                    Contained AS (
                        SELECT c1.*
                        FROM setNodes AS c1
                        JOIN setNodes AS c2 ON ST_Contains(c2.transformed_geom, c1.transformed_geom) 
                                            AND c1.set_id <> c2.set_id
                    ),
                    Containers AS (
                        SELECT c2.*
                        FROM Contained AS c1
                        JOIN setNodes AS c2 ON ST_Contains(c2.transformed_geom, c1.transformed_geom) 
                                            AND c1.set_id <> c2.set_id
                    )
                    SELECT * FROM Contained
                    UNION ALL
                    SELECT * FROM Containers
                )
                """
        ).format(
            relation_CTE_name=sql.Identifier(relation_CTE_name),
            src_CTE_name=sql.Identifier(src_CTE_name),
            tgt_CTE_name=sql.Identifier(tgt_CTE_name),
            utm=sql.Literal(g.utm),
        )

    return {"query": relational_query, "ctename": relation_CTE_name}
