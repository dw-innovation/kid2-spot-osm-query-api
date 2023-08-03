def cleanQuery(query):
    cleanedQuery = '\n'.join(line.strip() for line in query.split('\n'))
    return cleanedQuery
