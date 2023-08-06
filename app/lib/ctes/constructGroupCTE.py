# next line produces circular import error
# from ..constructor import constructQueryFromGraph 


def constructGroupCTE(node, area):
    # Extract setid and setname from the node, default to 'id' and 'name' if not provided

    setid = node.get('id', 'id')
    setname = node.get('n', 'name')

    groupName = f"group_{setid}_{setname}"

    # groupCTE = constructQueryFromGraph(node, area)

    ## to be wrapped in a CTE

    return ""
