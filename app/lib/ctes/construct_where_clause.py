from psycopg2 import sql


def construct_CTE_where_clause(filters, area=None):
    if not filters:
        return ""

    area_filter = sql.SQL("geom && (SELECT geom FROM envelope)")

    where_filters = [construct_filter(filter) for filter in filters]

    if area_filter:
        where_filters.insert(0, area_filter)

    whereClause = sql.SQL(" AND ").join(where_filters)

    return whereClause


def construct_filter(filter):
    key = filter.get("k", None)
    value = filter.get("v", None)
    operator = filter.get("op", None)

    if operator == "=":
        return sql.SQL("tags->> {key} = {value}").format(
            key=sql.Literal(key), value=sql.Literal(value)
        )

    if operator == "~":
        return sql.SQL("tags->> {key} ~ {value}").format(
            key=sql.Literal(key), value=sql.Literal(value)
        )

    if operator == ">":
        try:
            value = int(value)
            return sql.SQL(
                "CAST(SPLIT_PART(tags->> {key}, ' ', 1) AS FLOAT) > {value}"
            ).format(key=sql.Literal(key), value=sql.Literal(value))
        except ValueError:
            print(f"Error converting value '{value}' to integer for key '{key}'.")
            return ""

    if operator == "<":
        try:
            value = int(value)
            return sql.SQL(
                "CAST(SPLIT_PART(tags->> {key}, ' ', 1) AS FLOAT) < {value}"
            ).format(key=sql.Literal(key), value=sql.Literal(value))
        except ValueError:
            print(f"Error converting value '{value}' to integer for key '{key}'.")
            return ""
