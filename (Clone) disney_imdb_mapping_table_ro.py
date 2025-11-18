# Databricks notebook source
!pip install googlesearch-python tqdm duckduckgo_search

# COMMAND ----------

from duckduckgo_search import DDGS
from googlesearch import search
from tqdm.notebook import tqdm
from urllib.parse import unquote_plus
from pyspark.sql import SparkSession
import boto3
import io
import json
import pandas as pd
import time

# client = boto3.Session(
#   aws_access_key_id= dbutils.secrets.get(scope = "latam_ds", key = "aws_access_key_id"),
#   aws_secret_access_key= dbutils.secrets.get(scope = "latam_ds", key = "aws_secret_access_key"),
#   region_name="us-east-1",
# ).client('s3')

# tqdm.pandas(desc='IMDB Progress')

# COMMAND ----------

# query_tags = {
#     "team": "latam_data_science",
#     "project": "content_mapping",
#     "notebook": "disney_imdb_mapping_table",
#     "databricks_username": "javier.del.villar.pineda@disney.com",
#     "references_metric_store": "FALSE",
# }

# connection_dss_query = {
#     "sfUrl": "disneystreaming.us-east-1.privatelink.snowflakecomputing.com",
#     "sfUser": "LATAM_CW_NOTEBOOK_SERVICE_USER",
#     "sfPassword": dbutils.secrets.get(scope="latam_cw", key="latam_cw_notebook_service_user"),
#     "sfDatabase": "LATAM_CW",
#     "sfSchema": "BUILD",
#     "sfWarehouse": "LATAM_CW_SERVICE_WH",
#     "query_tag": json.dumps(query_tags),
# }

# connection_latam_query = {
#     "sfUrl": "research_latam_prd.us-east-1.snowflakecomputing.com",
#     "sfUser": "latam_disney_etl@disney.com",
#     "sfPassword": unquote_plus(dbutils.secrets.get(scope = "latam_ds", key = "snowflake_latam_key")),
#     "sfDatabase": "SNOWFLAKE_PRD",
#     "sfSchema": "STG_CI_BBUREU",
#     "sfWarehouse": "DEMO_WH",
#     "query_tag": json.dumps(query_tags),
# }

# snowflake_source_name = "net.snowflake.spark.snowflake"

# COMMAND ----------

# NO USARRRRRR

# test_connection_dss_query = {
#     "sfUrl": "disneystreaming.us-east-1.privatelink.snowflakecomputing.com",
#     "sfUser": "LATAM_CW_NOTEBOOK_OWNER_USER",
#     "sfPassword": dbutils.secrets.get(scope="latam_cw", key="latam_cw_notebook_owner_user"),
#     "sfDatabase": "LATAM_CW",
#     "sfSchema": "BUILD",
#     "sfWarehouse": "LATAM_CW_SERVICE_WH",
#     "query_tag": json.dumps(query_tags),
# }

# COMMAND ----------

# mapping_query = """
# with imdb_mapping as (
#     select
#         distinct
#         coalesce(ite.remappedto, ite.titleid) as imdb_id
#         , ite.titletype
#         , ite.originaltitle
#         , ite.year
#         , get_path(parse_json(ite.episodeinfo), 'episodeNumber') as season_episode_number
#         , get_path(parse_json(ite.episodeinfo), 'seasonNumber') as season_number
#         , replace(get_path(parse_json(ite.episodeinfo), 'seriesTitleId'), '""', '') as series_imdb_id
#         , replace(get_path(get(parse_json(ite.principalcastmembers), 0), 'nameId'), '""', '') as imdb_name_id
#         , ine.name
#     from
#         snowflake_prd.stg_recommender_system.imdb_title_essential ite
#         join snowflake_prd.stg_recommender_system.imdb_name_essential ine
#             on imdb_name_id = ine.nameid
#     where
#         ite.titletype in (
#             'movie', 'tvMovie', 'tvSpecial',
#             'short', 'tvShort',
#             'tvSeries', 'tvMiniSeries'
#         )
# ),

# imdb_program_ids as (
#     /* IMDB MOVIES PROGRAM */
#     with imdb_movies_program as (
#     select
#         dcm.programid
#         , dcm.seriesid
#         , coalesce(movies.imdb_id, movies_other.imdb_id) as program_imdb_id
#         , null as series_imdb_id
#     from
#         snowflake_prd.stg_recommender_system.dss_content_metadata dcm
#         left join imdb_mapping movies
#             on dcm.program_full_title = movies.originaltitle
#             and dcm.release_year = movies.year
#             and dcm.main_actor = movies.name
#             and dcm.content_class = 'movie'
#             and movies.titletype in ('movie', 'tvMovie', 'tvSpecial')
#         left join imdb_mapping movies_other
#             on dcm.program_full_title = movies_other.originaltitle
#             and dcm.release_year = movies_other.year
#             and dcm.main_actor = movies_other.name
#             and dcm.content_class = 'movie'
#             and movies_other.titletype in ('short', 'tvShort')
#     where
#         dcm.content_class = 'movie'
#         and program_imdb_id is not null
#     ),

#     /* IMDB SHORTS PROGRAM */
#     imdb_shorts_program as (
#     select
#         dcm.programid
#         , dcm.seriesid
#         , coalesce(shorts.imdb_id, shorts_other.imdb_id) as program_imdb_id
#         , null as series_imdb_id
#     from
#         snowflake_prd.stg_recommender_system.dss_content_metadata dcm
#         left join imdb_mapping shorts
#             on dcm.program_full_title = shorts.originaltitle
#             and dcm.release_year = shorts.year
#             and dcm.main_actor = shorts.name
#             and dcm.content_class = 'short-form'
#             and shorts.titletype in ('short', 'tvShort', 'tvSpecial')
#         left join imdb_mapping shorts_other
#             on dcm.program_full_title = shorts_other.originaltitle
#             and dcm.release_year = shorts_other.year
#             and dcm.main_actor = shorts_other.name
#             and dcm.content_class = 'short-form'
#             and shorts_other.titletype in ('movie', 'tvMovie')
#     where
#         dcm.content_class = 'short-form'
#         and program_imdb_id is not null
#     ),

#     /* IMDB SERIES PROGRAM */
#     imdb_series_program as (
#     select
#         dcm.programid
#         , dcm.seriesid
#         , null as program_imdb_id
#         , min(series.imdb_id) as series_imdb_id
#     from
#         snowflake_prd.stg_recommender_system.dss_content_metadata dcm
#         left join imdb_mapping series
#             on dcm.program_full_title = series.originaltitle
#             and dcm.release_year = series.year
#             and dcm.main_actor = series.name
#             and dcm.season_episode_number = 1
#             and dcm.season_number = 1
#             and dcm.content_class = 'series'
#             and series.titletype in ('tvSeries', 'tvMiniSeries')
#     where
#         dcm.content_class = 'series'
#         and series.imdb_id is not null
#     group by
#         1, 2
#     )

#     /* IMDB PROGRAM IDS TOGETHER 14888 matches of 37807 giving 39.37% coverage*/
#     select
#         distinct *
#     from 
#         imdb_movies_program
#     union all
#     select
#         distinct *
#     from 
#         imdb_shorts_program
#     union all
#     select
#         distinct *
#     from 
#         imdb_series_program
# ),

# /* IMDB ID MISSING TABLE */
# not_in_imdb_ids as (
#     select
#         dcm.programid
#         , dcm.seriesid
#         , dcm.program_radar_id
#         , dcm.season_radar_id
#         , dcm.series_radar_id
#     from
#         snowflake_prd.stg_recommender_system.dss_content_metadata dcm
#         left join imdb_program_ids ipi
#             on dcm.programid = ipi.programid
#     where
#         ipi.program_imdb_id is null
# ),

# /* PROGRAMIDS IN BBUREU, PLANNING AIRTABLE, CONTENT_MASTER */
# bb_pa_cm_program_ids as (
#     /* ALL METADATA SOURCES AGREE ON IMDB_IDS */
#     with all_agree_ids as (
#         select
#             distinct niii.*
#             , bbcw.imdb_id as bb_imdb_id
#             , cpa.imdb_id_str as pa_imdb_id
#             , cm.imdb_tconst_str as cm_imdb_id
#             , bb_imdb_id as program_imdb_id
#         from
#             not_in_imdb_ids niii
#             left join snowflake_prd.stg_ci_bbureu.catalog_view bbcw
#                 on niii.programid = bbcw.id
#             left join snowflake_prd.stg_content_inventory.planning_airtable cpa
#                 on niii.program_radar_id = cpa.radar_product_id_int
#             left join snowflake_prd.stg_content_master.content_master cm
#                 on niii.program_radar_id = cm.radar_product_id_int
#         where
#             bb_imdb_id is not null
#             and pa_imdb_id is not null
#             and cm_imdb_id is not null
#             and bb_imdb_id = pa_imdb_id
#             and bb_imdb_id = cm_imdb_id
#         )

#     select
#         programid
#         , seriesid
#         , program_imdb_id
#         , null series_imdb_id
#     from
#         all_agree_ids
# ),

# /* SERIESIDS IN BBUREU, PLANNING AIRTABLE, CONTENT_MASTER */
# bb_pa_cm_series_ids as (
#     /* ALL METADATA SOURCES AGREE ON IMDB_IDS */
#     with all_agree_ids as (
#         select
#             distinct niii.*
#             , bbcw.imdb_id as bb_imdb_id
#             , cpa.imdb_id_str as pa_imdb_id
#             , cm.imdb_tconst_str as cm_imdb_id
#             , bb_imdb_id as series_imdb_id
#         from
#             not_in_imdb_ids niii
#             left join snowflake_prd.stg_ci_bbureu.catalog_view bbcw
#                 on niii.seriesid = bbcw.id
#             left join snowflake_prd.stg_content_inventory.planning_airtable cpa
#                 on (
#                 niii.series_radar_id = cpa.radar_product_id_int
#                 or niii.series_radar_id = cpa.radar_group_number_int
#                 or niii.series_radar_id = cpa.radar_product_number_int
#                 )
#             left join snowflake_prd.stg_content_master.content_master cm
#                 on niii.series_radar_id = cm.radar_product_id_int
#         where
#             bb_imdb_id is not null
#             and pa_imdb_id is not null
#             and cm_imdb_id is not null
#             and bb_imdb_id = pa_imdb_id
#             and bb_imdb_id = cm_imdb_id
#     )

#     select
#         programid
#         , seriesid
#         , null program_imdb_id
#         , series_imdb_id
#     from
#         all_agree_ids
# ),

# /* SEASONIDS IN BBUREU, PLANNING AIRTABLE, CONTENT_MASTER */
# bb_pa_cm_season_ids as (
#     /* ALL METADATA SOURCES AGREE ON IMDB_IDS */
#     with all_agree_ids as (
#         select
#             distinct niii.*
#             , bbcw.imdb_id as bb_imdb_id
#             , cpa.imdb_id_str as pa_imdb_id
#             , cm.imdb_tconst_str as cm_imdb_id
#             , bb_imdb_id as series_imdb_id
#         from
#             not_in_imdb_ids niii
#             left join snowflake_prd.stg_ci_bbureu.catalog_view bbcw
#                 on niii.seriesid = bbcw.id
#             left join snowflake_prd.stg_content_inventory.planning_airtable cpa
#                 on (
#                 niii.season_radar_id = cpa.radar_product_id_int
#                 or niii.season_radar_id = cpa.radar_group_number_int
#                 or niii.season_radar_id = cpa.radar_product_number_int
#                 )
#             left join snowflake_prd.stg_content_master.content_master cm
#                 on niii.season_radar_id = cm.radar_product_id_int
#         where
#             bb_imdb_id is not null
#             and pa_imdb_id is not null
#             and cm_imdb_id is not null
#             and bb_imdb_id = pa_imdb_id
#             and bb_imdb_id = cm_imdb_id
#     )

#     select
#         programid
#         , seriesid
#         , null program_imdb_id
#         , series_imdb_id
#     from
#         all_agree_ids
# ),

# /* ALL TOGETHER */
# imdb_ids_mapped2 as (
#     with all_together as (
#         select
#             *
#         from
#             imdb_program_ids
#         union all
#         select
#             *
#         from
#             bb_pa_cm_program_ids
#         union all
#         select
#             *
#         from
#             bb_pa_cm_series_ids
#         union all
#         select
#             *
#         from
#             bb_pa_cm_season_ids
#     )

#     -- movies
#     select
#         min(programid) as programid
#         , seriesid
#         , program_imdb_id as imdb_id
#     from
#         all_together
#     where
#         series_imdb_id is null
#     group by
#         2, 3
#     union all
#     select
#         distinct null as programid
#         , seriesid
#         , series_imdb_id as imdb_id
#     from
#         all_together
#     where
#         program_imdb_id is null
# )

# select
#     distinct dcm.programid
#     , dcm.seriesid
#     , dcm.content_class
#     , dcm.is_animated
#     , dcm.program_full_title as full_title
#     , dcm.release_year
#     , dcm.originallanguage
#     , dcm.countryoforigin
#     , dcm.main_actor
#     , iim.imdb_id
# from
#     snowflake_prd.stg_recommender_system.dss_content_metadata dcm
#     left join imdb_ids_mapped2 iim
#         on dcm.programid = iim.programid
# where
#     dcm.content_class in ('movie', 'short-form')
# union all
# select
#     distinct null as programid
#     , dcm.seriesid
#     , dcm.content_class
#     , dcm.is_animated
#     , case
#         when dcm.seriesid = '3d6d8771-d888-4297-a925-99092f0024db' then 'Big Chibi 6: The Series (Shorts)'
#         else dcm.series_full_title
#         end as full_title
#     , dcm.release_year
#     , dcm.originallanguage
#     , dcm.countryoforigin
#     , dcm.main_actor
#     , case
#         when dcm.seriesid = 'd5bc5374-c956-4e91-9f28-22f87834ffbf' then null
#         when dcm.seriesid = '8bba1f82-a311-4b71-872b-1a9641c9150d' then null
#         when dcm.seriesid = 'f9884bb8-cd71-4e13-8103-7de8620221ef' then null
#         when dcm.seriesid = '50b4b23f-75ce-4047-9e25-71840f2b3303' then null
#         when dcm.seriesid = '37ead0f2-0a21-4f07-a4da-4e3aa71836d2' then null
#         when dcm.seriesid = 'b99e1cb7-fefc-4bc3-b204-f3b16e1a9852' then null
#         when dcm.seriesid = '3d6d8771-d888-4297-a925-99092f0024db' then null
#         else iim.imdb_id
#         end as imdb_id
# from
#     snowflake_prd.stg_recommender_system.dss_content_metadata dcm
#     left join imdb_ids_mapped2 iim
#         on dcm.seriesid = iim.seriesid
# where
#     dcm.content_class in ('series')
#     and dcm.season_episode_number = 1
#     and dcm.season_number = 1
# """

# COMMAND ----------

# mapping_data_temp = (
#     spark.read.format(snowflake_source_name)
#     .options(**connection_latam_query)
#     .option("query", mapping_query)
#     .load()
# ).toPandas()

# COMMAND ----------

# client.put_object(
#   Body=mapping_data_temp.to_parquet(),
#   Bucket='ds-latam-models',
#   Key=f'content_sets/mapping_data_raw.parquet'
# )

# COMMAND ----------

existing_mapping_query = """
select
    *
from
    snowflake_prd.stg_recommender_system.ds_imdb_mapping
"""

# COMMAND ----------

existing_mapping_data = (
    spark.read.format(snowflake_source_name)
    .options(**connection_latam_query)
    .option("query", existing_mapping_query)
    .load()
).toPandas()

# COMMAND ----------

existing_programids = existing_mapping_data.loc[existing_mapping_data['PROGRAMID'].notnull(), 'PROGRAMID']
new_programids = mapping_data_temp.loc[mapping_data_temp['PROGRAMID'].notnull(), 'PROGRAMID']
print(f'New programs: {len(new_programids) - len(existing_programids)}')

new_programs = list(set(new_programids) - set(existing_programids))
new_programs

# COMMAND ----------

existing_seriesids = existing_mapping_data.loc[existing_mapping_data['SERIESID'].notnull(), 'SERIESID']
new_seriesids = mapping_data_temp.loc[mapping_data_temp['SERIESID'].notnull(), 'SERIESID']
print(f'New series: {len(new_seriesids) - len(existing_seriesids)}')

new_series = list(set(new_seriesids) - set(existing_seriesids))
new_series

# COMMAND ----------

mapping_data = pd.concat([existing_mapping_data, mapping_data_temp.loc[mapping_data_temp['PROGRAMID'].isin(new_programs), :], mapping_data_temp.loc[mapping_data_temp['SERIESID'].isin(new_series), :]])

# COMMAND ----------

# mapping_data = client.get_object(
#   Bucket='ds-latam-models',
#   Key=f'content_sets/mapping_data_raw.parquet'
# )
# mapping_data = pd.read_parquet(io.BytesIO(mapping_data['Body'].read()))

# COMMAND ----------

display(mapping_data)

# COMMAND ----------

# MAGIC %md
# MAGIC MY VERSION
# MAGIC

# COMMAND ----------

# Buscamos en el Drive las pelis que tenemos mappeadas para sacar titulo y fecha de estreno


# COMMAND ----------

import time, re
from duckduckgo_search import DDGS

def imdb_id_from_search(title: str, year: int | str | None = None, pause: float = 1.0, max_results: int = 5):
    time.sleep(pause)
    q = f"{title} {year} site:imdb.com/title" if year else f"{title} site:imdb.com/title"
    try:
        results = list(DDGS().text(q, max_results=max_results))
        for r in results:
            href = r.get("href", "")
            m = re.search(r"/title/(tt\d{7,8})", href)
            if m:
                return m.group(1)  # ej: tt1375666
        return "ERROR"
    except Exception:
        return "ERROR"


# COMMAND ----------

pelis = [
    {"title": "Inception", "year": 2010},
    {"title": "The Matrix", "year": 1999},
    {"title": "Avatar", "year": 2009},
]

for p in pelis:
    p["IMDB_ID"] = imdb_id_from_search(p["title"], p.get("year"))


# COMMAND ----------

p

# COMMAND ----------

pelis

# COMMAND ----------

def imdb_search_ddgs(s):
    time.sleep(1)
    to_check = "tt"
    # s['RELEASE_YEAR'] = str(s['RELEASE_YEAR'])
    # s = [str(r) for r in s]
    search_query = "Avatar, IMDB"

    # First tries DuckDuckGo
    try:
        results = DDGS().text(search_query, max_results=1)
        result = [e for e in results[0]['href'].split('/') if e.startswith(to_check)][0]
        return result
        
    except:
        return 'ERROR'

def imdb_search_google(s):
    time.sleep(1)
    to_check = "tt"
    s['RELEASE_YEAR'] = str(s['RELEASE_YEAR'])
    s = [str(r) for r in s]
    search_query = ", ".join(list(s)) + ", IMDB"

    # First tries Google
    try:
        searches = search(search_query)
        urls = [urls for urls in searches]

        try:
            result = [e for e in urls[0].split('/') if e.startswith(to_check)][0]
            return result
        except:
            result = [e for e in urls[1].split('/') if e.startswith(to_check)][0]
            return result
        
    except:
        return 'ERROR'


# COMMAND ----------

# First with DuckDuckGo
imdb_mask = mapping_data['IMDB_ID'].isnull()
mapping_data.loc[imdb_mask, 'IMDB_ID'] =
 mapping_data.loc[imdb_mask, ['CONTENT_CLASS', 'FULL_TITLE', 'RELEASE_YEAR', 'MAIN_ACTOR']].progress_apply(imdb_search_ddgs, axis=1)

# Then with Google
imdb_mask = mapping_data['IMDB_ID'] == 'ERROR'
mapping_data.loc[imdb_mask, 'IMDB_ID'] = mapping_data.loc[imdb_mask, ['CONTENT_CLASS', 'FULL_TITLE', 'RELEASE_YEAR', 'MAIN_ACTOR']].progress_apply(imdb_search_google, axis=1)

# Again with DuckDuckGo
imdb_mask = mapping_data['IMDB_ID'] == 'ERROR'
mapping_data.loc[imdb_mask, 'IMDB_ID'] = mapping_data.loc[imdb_mask, ['CONTENT_CLASS', 'FULL_TITLE', 'RELEASE_YEAR', 'MAIN_ACTOR']].progress_apply(imdb_search_ddgs, axis=1)

# Again with Google
imdb_mask = mapping_data['IMDB_ID'] == 'ERROR'
mapping_data.loc[imdb_mask, 'IMDB_ID'] = mapping_data.loc[imdb_mask, ['CONTENT_CLASS', 'FULL_TITLE', 'RELEASE_YEAR', 'MAIN_ACTOR']].progress_apply(imdb_search_google, axis=1)

# COMMAND ----------

mapping_data['IMDB_ID'].value_counts()

# COMMAND ----------

mapping_data['IMDB_ID'] = mapping_data['IMDB_ID'].replace({'ERROR': None})

# COMMAND ----------

display(mapping_data)

# COMMAND ----------

# client.put_object(
#   Body=mapping_data.to_parquet(),
#   Bucket='ds-latam-models',
#   Key=f'content_sets/mapping_data.parquet'
# )

# COMMAND ----------

# client.put_object(
#   Body=mapping_data.to_csv(),
#   Bucket='ds-latam-models',
#   Key=f'content_sets/mapping_data.csv'
# )

# COMMAND ----------

# mapping_data = client.get_object(
#   Bucket='ds-latam-models',
#   Key=f'content_sets/mapping_data.parquet'
# )
# mapping_data = pd.read_parquet(io.BytesIO(mapping_data['Body'].read()))

# COMMAND ----------

mapping_data['IMDB_ID'].value_counts(dropna=False)

# COMMAND ----------

display(mapping_data)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
mapping_data_spark = spark.createDataFrame(mapping_data)

# COMMAND ----------

(
    mapping_data_spark.write.format(snowflake_source_name)
    .options(**connection_latam_query)
    .mode("overwrite")
    .option("dbtable", "SNOWFLAKE_PRD.STG_RECOMMENDER_SYSTEM.DS_IMDB_MAPPING")
    .save()
)

# COMMAND ----------

(
    mapping_data_spark.write.format(snowflake_source_name)
    .options(**connection_dss_query)
    .mode("overwrite")
    .option("dbtable", "LATAM_CW.BUILD.DS_IMDB_MAPPING")
    .save()
)
