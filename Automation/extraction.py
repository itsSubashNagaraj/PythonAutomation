from sql_metadata import Parser

def parse_sql_query(sql_parser):
    parser = Parser(sql_parser)
    return parser.tables, parser.columns, parser.tables_aliases, parser.columns_aliases

def map_columns_to_tables(columns, tables):
    table_columns_map = {table: [] for table in tables}
    for column in columns:
        for table in tables:
            if column.startswith(table + '.') or '.' + table + '.' in column:
                table_columns_map[table].append(column)
                break
            elif column.startswith(table.split('.')[-1]):
                table_columns_map[table].append(column)
                break
    return table_columns_map

def create_table_scripts(tables):
    return [f"CREATE TABLE {table} (\n    \n);" for table in tables]