def pyspark_column_rename(col_name):
    """
    This function removes special characters [" ,;{}(\n\t=)"] in Spark DataFrame column names
    before writing DataFrame to parquet file

    Arg:
        col_name (string): column name to format

    Returns:
        col_name (string): formatted column name.
    """

    col_name = col_name.replace(' ', '_').lower()

    to_remove = [',', ';', '{', '}', '(', ')', '\n', '\t', '=']
    schars = set(to_remove)

    col_name = ''.join([c for c in col_name if c not in schars])

    return col_name
