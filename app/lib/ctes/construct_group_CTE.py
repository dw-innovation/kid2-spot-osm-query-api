# next line produces circular import error
# from ..constructor import construct_query_from_graph


def construct_group_CTE(node, area):
    # Extract setid and setname from the node, default to 'id' and 'name' if not provided

    setid = node.get("id", "id")
    setname = node.get("n", "name")

    group_name = f"group_{setid}_{setname}"

    # groupCTE = construct_query_from_graph(node, area)

    ## to be wrapped in a CTE

    return ""
