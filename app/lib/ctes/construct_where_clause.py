def construct_CTE_where_clause(filters, area=None):
    """
    Function to construct the WHERE clause of the SQL query.
    """

    # If there are no filters, return an empty string.
    if not filters:
        return ""

    # If area is not None, construct area filter
    area_filter = "ST_Contains((SELECT geom FROM envelope), geom)"

    # Construct filters for each item in the 'filters' list
    where_filters = [construct_filter(filter) for filter in filters]

    # If area filter exists, prepend it to the list of where filters
    if area_filter:
        where_filters.insert(0, area_filter)

    # Concatenate filters with ' AND ' separator into a single where clause
    whereClause = " " + " AND ".join(where_filters)

    return whereClause


def construct_filter(filter):
    """
    Function to construct individual filter for the WHERE clause of the SQL query.
    """

    # Extract key, value, and operator from the filter
    key = filter.get("k", None)
    value = filter.get("v", None)
    operator = filter.get("op", None)

    # Construct filter condition based on operator
    if operator == "=":
        return f"tags->>'{key}' = '{value}'"

    if operator == "~":
        return f"tags->>'{key}' ~ '{value}'"

    if operator == ">":
        try:
            return f"CAST(tags->>'{key}' AS INTEGER) > {int(value)}"
        except ValueError:
            # Error handling when the value cannot be cast to an integer
            print(f"Error converting value '{value}' to integer for key '{key}'.")
            return ""

    if operator == "<":
        try:
            return f"CAST(tags->>'{key}' AS INTEGER) < {int(value)}"
        except ValueError:
            # Error handling when the value cannot be cast to an integer
            print(f"Error converting value '{value}' to integer for key '{key}'.")
            return ""
