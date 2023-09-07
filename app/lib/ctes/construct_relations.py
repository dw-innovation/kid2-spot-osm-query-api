from collections import defaultdict
from ..utils import distance_to_meters
from psycopg2 import sql


def construct_relations(imr):
    # Creating a mapping from node IDs to node names
    id_to_name = {node["id"]: node["n"] for node in imr["ns"]}

    # Using defaultdict to manage the join conditions for each target node
    join_conditions = defaultdict(list)

    # Loop through each edge in the input graph
    for edge in imr["es"]:
        # Get source and target node names
        src_name = id_to_name[edge["src"]]
        tgt_name = id_to_name[edge["tgt"]]

        # Get the type of relation between nodes
        type = edge["t"]

        # Process 'dist' (distance) type
        if type == "dist":
            dist = distance_to_meters(edge["dist"])  # Convert distance to meters

            # Formulate SQL join condition for distance
            condition = sql.SQL(
                "ST_DWithin({src_alias}.transformed_geom, {tgt_alias}.transformed_geom, {dist})"
            ).format(
                src_alias=sql.Identifier(src_name),
                tgt_alias=sql.Identifier(tgt_name),
                dist=sql.Literal(dist),
            )

        # Process 'cnt' (containment) type
        elif type == "cnt":
            # Formulate SQL join condition for containment
            condition = sql.SQL(
                "ST_Contains({src_alias}.transformed_geom, {tgt_alias}.transformed_geom)"
            ).format(
                src_alias=sql.Identifier(src_name),
                tgt_alias=sql.Identifier(tgt_name),
            )

        # Add condition to corresponding target node
        join_conditions[tgt_name].append(condition)

    # Process each target node and its conditions
    all_joins = []
    for tgt_name, conditions in join_conditions.items():
        # Chain multiple conditions with AND
        chained_conditions = sql.SQL(" AND ").join(conditions)

        # Formulate SQL JOIN clause
        join_clause = sql.SQL("JOIN {tgt} {tgt_alias} ON {conditions}").format(
            tgt=sql.Identifier(
                str(
                    next(
                        edge["tgt"]
                        for edge in imr["es"]
                        if id_to_name[edge["tgt"]] == tgt_name
                    )
                )
            ),
            tgt_alias=sql.Identifier(tgt_name),
            conditions=chained_conditions,
        )
        all_joins.append(join_clause)

    # Combine all JOIN clauses
    all_joins = sql.SQL(" ").join(all_joins)

    # Generate final SQL queries
    final_queries = []
    for node in imr["ns"]:
        node_name = node["n"]
        first_id = sql.Identifier(str(imr["ns"][0]["id"]))
        first_name = sql.Identifier(str(imr["ns"][0]["n"]))

        # Formulate SQL SELECT statement
        query_part = sql.SQL(
            """SELECT 
                {type} AS set_name, {name_alias}.osm_ids, {name_alias}.geom, {name_alias}.tags
                FROM {first_id} {first_name_alias}
                {joins}"""
        ).format(
            type=sql.Literal(node_name),
            name_alias=sql.Identifier(node_name),
            first_id=first_id,
            first_name_alias=first_name,
            joins=all_joins,
        )

        final_queries.append(query_part)

    # Combine all SQL queries using UNION ALL
    final_query = sql.SQL(" UNION ALL ").join(final_queries)

    return final_query
