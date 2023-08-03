from .constructClusterQuery import constructClusterSubquery

def constructSubqueries(intermediateRepresentation):
    subqueries = []
    nodes = intermediateRepresentation["ns"]
    for i in range(len(nodes)):
        subqueries.append(constructSubquery(nodes[i]))
    return subqueries

def constructSubquery(node):
    if node["t"] == "cluster":
        print("constructing cluster subquery")
        return constructClusterSubquery(node)