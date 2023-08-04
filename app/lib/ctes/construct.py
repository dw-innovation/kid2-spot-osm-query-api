from .constructNWRCTE import constructNWRCTE
from .constructClusterCTE import constructClusterCTE

def constructCTEs(intermediateRepresentation):
    ctes = []
    nodes = intermediateRepresentation["ns"]
    area = intermediateRepresentation["a"]

    for i in range(len(nodes)):
        cte = constructCTE(nodes[i], area)
        if i == 0:
            cte = "WITH " + cte
        ctes.append(cte)
    return ctes


def constructCTE(node, area):
    type = node["t"]
    if type == "cluster":
        return constructClusterCTE(node, area)
    elif type == "nwr":
        return constructNWRCTE(node, area)