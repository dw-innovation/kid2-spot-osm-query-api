from .ctes.construct import constructCTEs
from .unnest import unnest
from .utils import cleanQuery
from .relationalCTE import constructRelationalCTEs

def constructQuery(intermediateRepresentation):
    ctes = constructCTEs(intermediateRepresentation)
    relationalctes = constructRelationalCTEs(intermediateRepresentation)
    ctes.append(relationalctes)
    unnestedQuery = unnest(ctes)
    cleanedQuery = cleanQuery(unnestedQuery)
    
    return cleanedQuery