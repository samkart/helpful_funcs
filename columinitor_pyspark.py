"""
create dummy columns in a pyspark dataframe
"""

def columninitor(sdf, val=None, collist=[]):
    # type: (DataFrame, (int, str), list) -> DataFrame
    """
    Creates new columns with choice of value to existing data frames
    :param sdf: Spark DataFrame
    :param val: Value to input in the to-be-created columns
    :param collist: list; Column names for the columns to be created
    :return Spark DataFrame
    """
    from functools import reduce
    import pyspark.sql.functions as func
    import sys

    _range = xrange if sys.version[0] == '2' else range

    sdfcols = sdf.schema.names
    templist = []

    for k in collist:
        if k not in sdfcols:
            templist.append(k.lower())

    return reduce(lambda x, i: x.withColumn(templist[i], func.lit(val)), _range(len(templist)), sdf)

