import geojson

def constructCTEWhereClause(filters, area=None):
    if not filters:
        return ''
    
    areaFilter = buildAreaFilter(area)
    
    whereFilters = [buildFilter(filter) for filter in filters]

    if areaFilter:
        whereFilters.insert(0, areaFilter)

    whereClause = ' ' + ' AND '.join(whereFilters)
    return whereClause

def buildFilter(filter):
    key = filter.get('k', None)
    value = filter.get('v', None)
    operator = filter.get('op', None)

    if operator == "=":
        return f"tags->>'{key}' = '{value}'"
    
    if operator == "!=":
        return f"tags->>'{key}' != '{value}'"
    
    if operator == "~":
        return f"tags->>'{key}' ~ '{value}'"
    
    if operator == ">":
        try:
            return f"CAST(tags->>'{key}' AS INTEGER) > {int(value)}"
        except ValueError:
            print(f"Error converting value '{value}' to integer for key '{key}'.")
            return ''
        
def buildAreaFilter(area):
    if not area:
        return ''
    
    type = area.get('t', None)
    geom = area.get('geom', None)

    if type == 'bbox':
        areaFilter = f"ST_Contains(ST_MakeEnvelope({area['geom'][0]}, {area['geom'][1]}, {area['geom'][2]}, {area['geom'][3]}, 4326), geom)"
    elif type == 'polygon':
        geojson_str = geojson.dumps(geom)
        areaFilter = r"ST_Contains(ST_GeomFromGeoJSON('" + geojson_str + r"'), geom)".replace('\"', '"')


    else:
        return ''

    return areaFilter
