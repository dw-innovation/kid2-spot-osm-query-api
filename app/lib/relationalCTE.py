def constructRelationalCTEs(intermediateRepresentation):
    nodes = intermediateRepresentation["ns"]
    edges = intermediateRepresentation["es"]

    relations = [buildRelation(edge, nodes) for edge in edges]

    return "RelationalCTE AS (" + " UNION ALL ".join(relations) + ")"


def buildRelation(edge, nodes):
    type = edge["t"]
    sourceId = edge["src"]
    targetId = edge["tgt"]

    relationQuery = ""

    if type == "dist":
        dist = edge["dist"]
        srcSet = next((item for item in nodes if item['id'] == sourceId), None)
        srcCTEName = f"nwr_{srcSet['id']}_{srcSet['n']}"
        tgtSet = next((item for item in nodes if item['id'] == targetId), None)
        tgtCTEName = f"nwr_{tgtSet['id']}_{tgtSet['n']}"

        relationQuery = f"""SELECT
                                '{srcSet['id']}_{srcSet['n']}' AS srcid,
                                '{tgtSet['id']}_{tgtSet['n']}' AS tgtid,
                                src.nodes AS srcnodes,
                                tgt.nodes AS tgtnodes,
                                src.geom AS srcgeom,
                                tgt.geom AS tgtgeom
                            FROM {srcCTEName} AS src
                            LEFT JOIN {tgtCTEName} AS tgt
                            ON ST_DWithin(ST_Transform(src.geom, 3857), ST_Transform(tgt.geom, 3857), {dist})
                            WHERE src.nodes IS NOT NULL AND tgt.nodes IS NOT NULL"""

    
    return relationQuery