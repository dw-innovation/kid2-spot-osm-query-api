from .ctes.construct import constructCTEs
from .unnest import unnest
from .utils import cleanQuery
from .ctes.constructRelationalCTE import constructRelationalCTEs

def constructQueryFromGraph(intermediateRepresentation):
    """
    This function takes a JSON representation of the intermediate query,
    constructs common table expressions (CTEs), unnests the CTEs to create individual rows,
    and cleans the resulting query.

    :param intermediateRepresentation: JSON representation of the intermediate query
    :type intermediateRepresentation: JSON object
    :return: SQL query string
    :rtype: str
    """
    
    try:
        # Build all CTEs, so subqueries that we can then reference in the relational CTE
        ctes = constructCTEs(intermediateRepresentation)
        
        # Build the relational CTEs
        relationalctes = constructRelationalCTEs(intermediateRepresentation)
        
        # Add the relational CTEs to the list of CTEs
        ctes.append(relationalctes)
        
        # Unnest the CTEs to unfold the array of nodes stored in the nodes column into individual rows
        unnestedQuery = unnest(ctes)
        
        # Clean the query to remove unnecessary whitespace
        cleanedQuery = cleanQuery(unnestedQuery)
        
        return cleanedQuery
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
