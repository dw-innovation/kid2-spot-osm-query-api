import re
from psycopg2 import sql

from ..utils import distance_to_meters

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

    if key in ["height", "width", "length"] or operator in [">", "<"]:
        value = distance_to_meters(value)

    if value == "***any***":
        return sql.SQL("tags ? {key}").format(key=sql.Literal(key))

    if operator == "~":
        value = sanitize_for_regex(value)
        sql_template = sql.SQL(
            "LOWER(REGEXP_REPLACE(tags->>{key}, '[^A-Za-z0-9]', '', 'g')) ~ LOWER({value})"
        ).format(
            key=sql.Literal(key),
            value=sql.Literal(value)
        )

    elif operator in [">", "<"]:
        sql_template = sql.SQL(
            """
            CASE 
                WHEN tags->> {key} ~ '^[0-9]+(\\.[0-9]+)?$'
                THEN CAST(tags->> {key} AS FLOAT) {operator} {value}
                ELSE FALSE 
            END
            """
        ).format(
            key=sql.Literal(key),
            operator=sql.SQL(operator),
            value=sql.Literal(value)
        )

    else:
        sql_template = sql.SQL("tags->> {key} = {value}").format(
            key=sql.Literal(key),
            value=sql.Literal(value)
        )

    return sql_template
