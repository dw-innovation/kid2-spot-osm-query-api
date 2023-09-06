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
                    ),
                    Distances AS (
                        SELECT c1.osm_ids AS src_id, array_agg(DISTINCT c2.osm_ids) AS tgt_ids,
                            c1.tags AS tags, c1.geom AS geom
                        FROM setNodes AS c1
                        JOIN setNodes AS c2 ON ST_DWithin(c1.transformed_geom, c2.transformed_geom, {dist_in_meters})
                        AND c1.set_id <> c2.set_id
                        GROUP BY c1.osm_ids, c1.tags, c1.geom
                    ),
                    ReverseDistances AS (
                        SELECT c2.osm_ids AS tgt_id, array_agg(DISTINCT c1.osm_ids) AS src_ids,
                            c2.tags AS tags, c2.geom AS geom
                        FROM setNodes AS c1
                        JOIN setNodes AS c2 ON ST_DWithin(c1.transformed_geom, c2.transformed_geom, {dist_in_meters})
                        AND c1.set_id <> c2.set_id
                        GROUP BY c2.osm_ids, c2.tags, c2.geom
                    ),
                    Combined AS (
                        SELECT 
                            COALESCE(d.src_id, rd.tgt_id) AS node_id,
                            COALESCE(d.tags, rd.tags) AS tags,
                            COALESCE(d.geom, rd.geom) AS geom,
                            d.tgt_ids AS src_nearby,
                            rd.src_ids AS tgt_nearby
                        FROM Distances d
                        FULL OUTER JOIN ReverseDistances rd ON d.src_id = rd.tgt_id
                    )
                    SELECT 
                        node_id, tags, geom,
                        ARRAY(
                            SELECT DISTINCT unnest(ARRAY_CAT(COALESCE(src_nearby, ARRAY[]::bigint[]), COALESCE(tgt_nearby, ARRAY[]::bigint[])))
                        ) AS nearby_node_ids
                    FROM Combined
            )"""
        ).format(
            relation_CTE_name=sql.Identifier(relation_CTE_name),
            src_CTE_name=sql.Identifier(src_CTE_name),
            tgt_CTE_name=sql.Identifier(tgt_CTE_name),
            utm=sql.Literal(g.utm),
            dist_in_meters=sql.Literal(dist_in_meters),
            src_id=sql.Literal(source_id),
            target_id=sql.Literal(target_id),
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
            """{relation_CTE_name} AS (
                    WITH setNodes AS (
                        SELECT *, 'src' AS origin FROM {src_CTE_name}
                        UNION ALL
                        SELECT *, 'tgt' AS origin FROM {tgt_CTE_name}
                    ),
                    Contained AS (
                        SELECT c1.osm_ids AS contained_id, c1.tags AS contained_tags, c1.geom AS contained_geom, array_agg(c2.osm_ids) AS container_ids
                        FROM setNodes AS c1
                        JOIN setNodes AS c2 ON ST_Contains(c2.transformed_geom, c1.transformed_geom) 
                                            AND c1.set_id <> c2.set_id
                        GROUP BY c1.osm_ids, c1.tags, c1.geom
                    ),
                    Containers AS (
                        SELECT c2.osm_ids AS container_id, c2.tags AS container_tags, c2.geom AS container_geom, array_agg(c1.osm_ids) AS contained_ids
                        FROM setNodes AS c1
                        JOIN setNodes AS c2 ON ST_Contains(c2.transformed_geom, c1.transformed_geom) 
                                            AND c1.set_id <> c2.set_id
                        GROUP BY c2.osm_ids, c2.tags, c2.geom
                    ),
                    Combined AS (
                        SELECT 
                            COALESCE(cd.contained_id, cn.container_id) AS node_id,
                            COALESCE(cd.contained_tags, cn.container_tags) AS tags,
                            COALESCE(cd.contained_geom, cn.container_geom) AS geom,
                            cd.container_ids AS src_nearby,
                            cn.contained_ids AS tgt_nearby
                        FROM Contained cd
                        FULL OUTER JOIN Containers cn ON cd.contained_id = cn.container_id
                    )
                    SELECT 
                        node_id, tags, geom,
                        ARRAY(
                            SELECT DISTINCT unnest(ARRAY_CAT(COALESCE(src_nearby, ARRAY[]::bigint[]), COALESCE(tgt_nearby, ARRAY[]::bigint[])))
                        ) AS nearby_node_ids
                    FROM Combined
                )
                """
        ).format(
            relation_CTE_name=sql.Identifier(relation_CTE_name),
            src_CTE_name=sql.Identifier(src_CTE_name),
            tgt_CTE_name=sql.Identifier(tgt_CTE_name),
            utm=sql.Literal(g.utm),
        )

    return {"query": relational_query, "ctename": relation_CTE_name}
