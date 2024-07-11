from flask import g
from .construct_search_area import construct_search_area_cte
from .construct_nwrs import construct_nwr_cte
from .construct_cluster import construct_cluster_cte


def construct_ctes(spot_query):
    # Initialize an empty list to hold the CTEs
    ctes = []

    # Extract nodes from the SPOT query
    nodes = spot_query["nodes"]

    # Construct envelope CTE
    envelope_cte = construct_search_area_cte(g.area["type"])

    ctes.append(envelope_cte)
    
    # Iterate over the nodes to construct each CTE
    for i in range(len(nodes)):
        cte = construct_cte(nodes[i])

        # Append the CTE to the list
        ctes.append(cte)

    return ctes


def construct_cte(node):
    # Get the type of the node
    type = node["type"]

    # Depending on the type of the node, construct the appropriate type of CTE
    if type == "cluster":
        return construct_cluster_cte(node)

    elif type == "nwr":
        return construct_nwr_cte(node)
