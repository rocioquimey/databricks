# Databricks notebook source
# MAGIC %pip install json

# COMMAND ----------

import json

# COMMAND ----------


query_tags = {
    "team": "latam_BI",
    "project": "snowflake_latam_test",
    "notebook": "snowflake_latam_test",
    "databricks_username": "rocio.ramos@disney.com",
    "references_metric_store": "FALSE",
}

connection_latam_query = {
    "sfUrl": "research_latam_prd.us-east-1.snowflakecomputing.com",
    "sfUser": "latam_disney_etl@disney.com",
    # "sfUser": "rocio.ramos@disney.com",
    "sfPassword": dbutils.secrets.get(scope = "latam_bi", key = "snowflake_latam_key"),
    "sfDatabase": "SNOWFLAKE_PRD",
    "query_tag": json.dumps(query_tags),
}

snowflake_source_name = "net.snowflake.spark.snowflake"

# COMMAND ----------

# Colocar query acá

sf_query1 = """
    select top 5
* from
MARKETING_LATAM.FOUNDATION.BOX_OFFICE_COMSCORE_CONSOLIDATED_VW
;"""


sf_query2 = """
    select 
        distinct imdb_id
        , season
        , original_aired_date
        , latam_release_date
        , replace(parse_json(genres)[0], '"', '') as imdb_genre
    from 
        snowflake_prd.stg_ci_workspace.t_catalog_view_updated_correcciones cv
        left join snowflake_prd.stg_recommender_system.imdb_title_essential te 
            on te.titleid = cv.imdb_id
    where 
        platform_name in ('Netflix', 'Max', 'HBO Max', 'Amazon  Prime Video')
        and data_date between '2021-01-01' and '2024-12-31'
        and type = 'Tv Show'
;
"""


# COMMAND ----------

# # Colocar query acá

# sf_query1 = """
#     select 
#         distinct imdb_id
#         , season
#         , original_aired_date
#         , latam_release_date
#         , replace(parse_json(genres)[0], '"', '') as imdb_genre
#     from 
#         snowflake_prd.stg_ci_workspace.t_catalog_view_updated_correcciones cv
#         left join snowflake_prd.stg_recommender_system.imdb_title_essential te 
#             on te.titleid = cv.imdb_id
#     where 
#         platform_name in ('Netflix', 'Max', 'HBO Max', 'Amazon  Prime Video')
#         and data_date between '2021-01-01' and '2024-12-31'
#         and type = 'Tv Show'
# ;
# """


# sf_query2 = """
#     select 
#         distinct imdb_id
#         , season
#         , original_aired_date
#         , latam_release_date
#         , replace(parse_json(genres)[0], '"', '') as imdb_genre
#     from 
#         snowflake_prd.stg_ci_workspace.t_catalog_view_updated_correcciones cv
#         left join snowflake_prd.stg_recommender_system.imdb_title_essential te 
#             on te.titleid = cv.imdb_id
#     where 
#         platform_name in ('Netflix', 'Max', 'HBO Max', 'Amazon  Prime Video')
#         and data_date between '2021-01-01' and '2024-12-31'
#         and type = 'Tv Show'
# ;
# """


# COMMAND ----------

# Acá se ejecuta la query

sf_data1 = (
    spark.read.format(snowflake_source_name)
    .options(**connection_latam_query)
    .option("query", sf_query1)
    .load()
)

sf_data2 = (
    spark.read.format(snowflake_source_name)
    .options(**connection_latam_query)
    .option("query", sf_query2)
    .load()
)

# COMMAND ----------

display(sf_data1)

# COMMAND ----------

# Si necesitás trabajar en pandas tenés que convertir la tabla a un Dataframe

sf_data = sf_data.toPandas()

# COMMAND ----------

sf_data

# COMMAND ----------




spark_df = spark.createDataFrame(df_final)
(
    spark_df.write.format(snowflake_source_name)
    .options(**connection_dss_query)
    .mode("overwrite")
    .option("dbtable", "LATAM_CW.BUILD.MB_ACTUALVSMG")
    .save()
)

# COMMAND ----------

data = (
    spark.read.format(snowflake_source_name)
    .options(**connection_dss_query)
    .option("query", sfquery)
    .load()
)
metric_store_df = data.toPandas()

