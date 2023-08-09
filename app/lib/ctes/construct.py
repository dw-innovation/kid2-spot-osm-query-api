from .construct_group_CTE import construct_group_CTE
from .construct_NWR_CTE import construct_NWR_CTE
from .construct_cluster_CTE import construct_cluster_CTE


def construct_ctes(intermediate_representation):
    """
    Constructs a list of Common Table Expressions (CTEs) using the nodes and
    area data from the intermediate representation of a SQL query.

    :param intermediate_representation: A dict containing "ns" (nodes) and "a" (area).
    :type intermediate_representation: dict
    :return: A list of CTEs represented as strings.
    :rtype: list
    """

    # Initialize an empty list to hold the CTEs
    ctes = []

    # Extract nodes and area from the intermediate representation
    nodes = intermediate_representation["ns"]
    area = intermediate_representation["a"]

    # Iterate over the nodes to construct each CTE
    for i in range(len(nodes)):
        cte = constructCTE(nodes[i], area)

        # If this is the first CTE, prepend it with "WITH" for proper SQL syntax
        if i == 0:
            cte = "WITH " + cte

        # Append the CTE to the list
        ctes.append(cte)

    return ctes


def constructCTE(node, area):
    """
    Constructs a Common Table Expression (CTE) based on the provided node and area.

    :param node: A dict containing information for CTE construction.
    :type node: dict
    :param area: A parameter used in the WHERE clause for area filtering.
    :type area: str, int or float
    :return: A CTE represented as a string.
    :rtype: str
    """

    # Get the type of the node
    type = node["t"]

    # Depending on the type of the node, construct the appropriate type of CTE
    if type == "cluster":
        return construct_cluster_CTE(node, area)
    elif type == "nwr":
        return construct_NWR_CTE(node, area)
    elif type == "group":
        return construct_group_CTE(node, area)
