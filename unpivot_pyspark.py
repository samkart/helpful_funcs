"""
unpivot pyspark dataframe
"""

def melt_sdf(sdf, id_vars=[], val_vars=[], var_name='var', val_name='val'):
    # type: (DataFrame, list, list, str, str) -> DataFrame
    """
    Convert :class:`DataFrame` from wide to long format. Transpose down.
    :var sdf: Spark DataFrame
    :var id_vars: a list; index columns; columns not to be touched in pivot down
    :var val_vars: a list; columns to pivot down
    :var var_name: column name for key column
    :var val_name: column name for value column
    :return :class: DataFrame
    :usage:
    >> mapcols = ['wamd','dr','dme','brvo','crvo','re','other']
    >> blah_sdf = melt_sdf(mysdf, [k for k in mysdf.schema.names if k not in mapcols], mapcols, 'ind_key', 'ind_flag')
    """
    import pyspark.sql.functions as func

    # Spark SQL supports only homogeneous columns
    val_dtypes = [dtyp for colnm, dtyp in sdf.dtypes if colnm in val_vars]
    assert len(set(val_dtypes)) == 1, "All val_vars have to be of the same data type"

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = func.array(*(func.struct(func.lit(k).alias(var_name), func.col(k).alias(val_name))
                                  for k in val_vars)
                                )

    # Add to the DataFrame and explode
    _tmp = sdf.withColumn("_vars_and_vals", func.explode(_vars_and_vals))

    # Create list of required column names to pass to select
    cols = id_vars + [func.col("_vars_and_vals")[x].alias(x) for x in [var_name, val_name]]

    return _tmp.select(*cols)

