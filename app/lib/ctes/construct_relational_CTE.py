from flask import g
from ..utils import distance_to_meters


def construct_relational_CTEs(intermediate_representation):
    # Extract nodes and edges from the input intermediate representation
    nodes = intermediate_representation["ns"]
    edges = intermediate_representation["es"]

    # Construct relations for each edge
    relations = [construct_relation(edge, nodes) for edge in edges]

    relationalCTEs = "WITH " + ", ".join(relation["query"] for relation in relations)

    union_clauses = " UNION ALL ".join(
        f"SELECT * FROM {relation['ctename']}" for relation in relations
    )
    return f"RelationalCTE AS ({relationalCTEs} {union_clauses} )"


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
        relational_query = f"""{relation_CTE_name} AS (
                                    WITH UnionCTE AS (
                                        SELECT * FROM {src_CTE_name}
                                        UNION ALL
                                        SELECT * FROM {tgt_CTE_name}
                                    )
                                    SELECT * FROM UnionCTE AS c1
                                    WHERE EXISTS (
                                        SELECT 1 
                                        FROM UnionCTE AS c2
                                        WHERE ST_DWithin(ST_Transform(c1.geom, {g.utm}), ST_Transform(c2.geom, {g.utm}), {dist_in_meters})
                                        AND c1.setid <> c2.setid)
                                    )"""

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

        # Construct the relation query using the defined CTE names
        relational_query = f"""
                {relation_CTE_name} AS (
                    WITH UnionCTE AS (
                        SELECT * FROM {src_CTE_name}
                        UNION ALL
                        SELECT * FROM {tgt_CTE_name}
                    ),
                    Contained AS (
                        SELECT c1.*
                        FROM UnionCTE AS c1
                        WHERE EXISTS (
                            SELECT 1 
                            FROM UnionCTE AS c2
                            WHERE ST_Contains(ST_Transform(c2.geom, {g.utm}), ST_Transform(c1.geom, {g.utm}))
                            AND c1.setid <> c2.setid
                        )
                    ),
                    Containers AS (
                        SELECT c2.*
                        FROM UnionCTE AS c2
                        WHERE EXISTS (
                            SELECT 1 
                            FROM Contained
                            WHERE ST_Contains(ST_Transform(c2.geom, {g.utm}), ST_Transform(Contained.geom, {g.utm}))
                            AND c2.setid <> Contained.setid
                        )
                    )
                    SELECT * FROM Contained
                    UNION ALL
                    SELECT * FROM Containers
                )
                """
    query = {
        "query": relational_query,
        "ctename": relation_CTE_name,
    }

    return query
