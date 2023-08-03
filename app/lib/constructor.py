from .subqueries.construct import constructSubqueries
from .unnest import unnest
from .utils import cleanQuery

def constructQuery(intermediateRepresentation):
    subqueries = constructSubqueries(intermediateRepresentation)
    unnestedQuery = unnest(subqueries)
    cleanedQuery = cleanQuery(unnestedQuery)
    
    return cleanedQuery