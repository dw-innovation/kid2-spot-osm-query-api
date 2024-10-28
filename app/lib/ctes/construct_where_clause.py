import re
from psycopg2 import sql

def construct_cte_where_clause(filters):
    if not filters:
        return ""

    area_filter = sql.SQL("geom && (SELECT geom FROM envelope)")
    where_filters = [construct_filter(f) for f in filters]

    if area_filter:
        where_filters.insert(0, area_filter)

    return sql.SQL(" AND ").join(where_filters)


def sanitize_for_regex(value):
    if isinstance(value, str):
        return re.sub(r'[^A-Za-z0-9]', '', value)
    return value


def construct_filter(filter):
    if "and" in filter:
        return sql.SQL("({})").format(
            sql.SQL(" AND ").join([construct_filter(f) for f in filter["and"]])
        )

    if "or" in filter:
        return sql.SQL("({})").format(
            sql.SQL(" OR ").join([construct_filter(f) for f in filter["or"]])
        )

    key = filter.get("key", "key")
    operator = filter.get("operator", "operator")
    value = filter.get("value", "value")

    if operator == "~":
        value = sanitize_for_regex(value)
        sql_template = "LOWER(REGEXP_REPLACE(tags->>{key}, '[^A-Za-z0-9]', '', 'g')) ~ LOWER({value})"
    else:
        sql_template = {
            "=": "tags->> {key} = {value}",
            ">": "CAST(SPLIT_PART(tags->> {key}, ' ', 1) AS FLOAT) > {value}",
            "<": "CAST(SPLIT_PART(tags->> {key}, ' ', 1) AS FLOAT) < {value}",
        }.get(operator)

    if not sql_template:
        return ""

    if value == "***any***":
        sql_template = "tags ? {key}"

    return sql.SQL(sql_template).format(
        key=sql.Literal(key),
        value=sql.Literal(value),
    )
