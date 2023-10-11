from .construct_search_area_CTE import construct_search_area_CTE
from .construct_NWR_CTE import construct_NWR_CTE
from .construct_cluster_CTE import construct_cluster_CTE


def construct_ctes(intermediate_representation):
    # Initialize an empty list to hold the CTEs
    ctes = []

    # Extract nodes and area from the intermediate representation
    nodes = intermediate_representation["nodes"]
    area = intermediate_representation["area"]

    # Construct envelope CTE
    envelope_cte = construct_search_area_CTE()

    ctes.append(envelope_cte)

    # Iterate over the nodes to construct each CTE
    for i in range(len(nodes)):
        cte = constructCTE(nodes[i], area)

        # Append the CTE to the list
        ctes.append(cte)

    return ctes


def constructCTE(node, area):
    # Get the type of the node
    type = node["type"]

    # Depending on the type of the node, construct the appropriate type of CTE
    if type == "cluster":
        return construct_cluster_CTE(node, area)
    elif type == "nwr":
        return construct_NWR_CTE(node, area)
