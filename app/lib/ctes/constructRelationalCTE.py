def constructRelationalCTEs(intermediateRepresentation):
    # Extract nodes and edges from the input intermediate representation
    nodes = intermediateRepresentation["ns"]
    edges = intermediateRepresentation["es"]

    # Construct relations for each edge
    relations = [constructRelation(edge, nodes) for edge in edges]

    # Return a single string that represents all the relations as a UNION of individual relational CTEs
    return "RelationalCTE AS (" + " UNION ALL ".join(relations) + ")"

# Function to construct an individual relation
def constructRelation(edge, nodes):
    # Extract type, sourceId and targetId from the input edge
    type = edge["t"]
    sourceId = edge["src"]
    targetId = edge["tgt"]

    # Initialize an empty string to construct the relation query
    relationQuery = ""

    # Check if the edge type is "dist"
    if type == "dist":
        # Create relation and source/target CTE names based on sourceId and targetId
        relationCTEName = f"Dist_{sourceId}_{targetId}"
        dist = edge["dist"]

        # Find the source node and its type from the nodes list
        srcType = next((item for item in nodes if item['id'] == sourceId), None)['t']
        srcSet = next((item for item in nodes if item['id'] == sourceId), None)
        srcCTEName = f"{srcType}_{srcSet['id']}_{srcSet['n']}"

        # Find the target node and its type from the nodes list
        tgtType = next((item for item in nodes if item['id'] == targetId), None)['t']
        tgtSet = next((item for item in nodes if item['id'] == targetId), None)
        tgtCTEName = f"{tgtType}_{tgtSet['id']}_{tgtSet['n']}"

        # Construct the relation query using the defined CTE names and distance
        relationQuery = f"""WITH {relationCTEName} AS (
                                SELECT * FROM {srcCTEName}
                                UNION ALL
                                SELECT * FROM {tgtCTEName})
                            SELECT * FROM {relationCTEName} AS c1
                            WHERE EXISTS (
                                SELECT 1 
                                FROM {relationCTEName} AS c2
                                WHERE ST_DWithin(ST_Transform(c1.geom,3857), ST_Transform(c2.geom,3857), {dist})
                                AND c1.setid <> c2.setid
                            )"""

    return relationQuery
