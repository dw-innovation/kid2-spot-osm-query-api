from .construct_search_area_cte import construct_search_area_cte
from .construct_NWR_cte import construct_NWR_cte
from .construct_cluster_cte import construct_cluster_cte


def construct_ctes(spot_query):
    # Initialize an empty list to hold the CTEs
    ctes = []

    # Extract nodes and area from the intermediate representation
    nodes = spot_query["nodes"]
    area = spot_query["area"]

    # Construct envelope CTE
    envelope_cte = construct_search_area_cte(area.get("type"), area.get("value"))

    ctes.append(envelope_cte)

    # Iterate over the nodes to construct each CTE
    for i in range(len(nodes)):
        cte = construct_cte(nodes[i], area)

        # Append the CTE to the list
        ctes.append(cte)

    return ctes


def construct_cte(node, area):
    # Get the type of the node
    type = node["type"]

    # Depending on the type of the node, construct the appropriate type of CTE
    if type == "cluster":
        return construct_cluster_cte(node, area)

    elif type == "nwr":
        return construct_NWR_cte(node, area)
