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

# existing_mapping_query = """
# select
#     *
# from
#     snowflake_prd.stg_recommender_system.ds_imdb_mapping
# """

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

def imdb_search_ddgs(s):
    time.sleep(1)
    to_check = "tt"
    # s['RELEASE_YEAR'] = str(s['RELEASE_YEAR'])
    s = [str(r) for r in s]
    search_query = ", ".join(list(s)) + ", IMDB"

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
    # s['RELEASE_YEAR'] = str(s['RELEASE_YEAR'])
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

pip install ddgs

# COMMAND ----------

imdb_search_ddgs('Intensamente 2')

# COMMAND ----------

imdb_mask = ['Intensamente 2',
'Deadpool & Wolverine',
'Mi villano favorito 4',
'Moana 2',
'Kung Fu Panda 4',
'Godzilla y Kong: El Nuevo Imperio',
'El planeta de los simios: nuevo reino',
'Robot Salvaje',
'Venom: el ultimo baile',
'Garfield: fuera de casa',
'Romper el círculo',
'Mufasa: El Rey Leon',
'Beetlejuice Beetlejuice',
'Con todos menos contigo',
'Guason 2: Folie a Deux',
'Duna: Parte 2',
'La sustancia',
'Ghostbusters: Apocalipsis fantasma',
'Gladiador 2',
'Furiosa',
'Demon Slayer: Kimetsu No Yaiba - Rumbo al entrenamiento de los pilares',
'La Primera Profecía',
'Coraline 15 Aniversario',
'Wish: El poder de los deseos',
'No hables con extraños',
'Bob Marley: La Leyenda',
'¡Patos!',
'Sonrie 2',
'El niño y la garza',
'Chicas pesadas',
'Aguas siniestras',
'Baghead: habla con los muertos',
'De noche con el diablo',
'Desafiantes',
'Mi amigo robot',
'Profesión peligro',
'Tarot de la muerte',
'The Chosen: Temporada 4 - Episodio 1 y 2',
'Vidas pasadas',
'Lazos de vida',
'Los extraños: capitulo 1',
'Observados',
'Masha y el Oso: ¡El doble de diversión!',
'El último conjuro',
'Jack en la caja maldita 3: El ascenso',
'Rescate imposible',
'Zona de interés',
'No te sueltes',
'El Apocalipsis de San Juan',
'Instinto maternal',
'La Habitacion de al lado',
'Blackpink World Tour',
'Carnada',
'Inseparables',
'Luca',
'The Chosen Temporada 4: Capitulos 3 y 4',
'Tipos de gentileza',
'Daft Punk & Leiji Matsumoto: Interstella 5555',
'Horrorland',
'Solo Leveling: Segundo Despertar',
'El Aprendiz',
'El viejo roble',
'Gurren Lagann The movie: childhood´s end',
'El aroma del pasto recién cortado',
'Bad Boys Hasta la muerte',
'Un Lugar en Silencio: Día Uno',
'Alien: Romulus',
'Tornados',
'Codigo: Traje Rojo',
'Amigos imaginarios',
'Madame Web',
'Pobres Criaturas',
'Terrifier 3',
'El Tiempo que Tenemos',
'Beekeeper: Sentencia de muerte',
'Guerra Civil',
'Arthur: Una amistad sin límites',
'Kraven El Cazador',
'Inmaculada',
'Argylle: agente secreto',
'La trampa',
'Ferrari',
'Hachiko 2 siempre a tu lado',
'Longlegs: coleccionista de almas',
'Abigail',
'Imaginario: Juguete Diabólico',
'Haikyu!! La batalla del basurero',
'Anatomía de una caída',
'Un gato con suerte',
'El cuervo',
'Sismo magnitud 9.5',
'Mi amigo el pingüino',
'Jung Kook: I Am Still',
'Priscilla',
'La otra cara de la luna',
'Alicia en el país de las pesadillas',
'Hellboy: The Crooked Man',
'Maxxxine',
'Suga - Agust D Tour´D -Day The Movie',
'El Conde de Montecristo',
'My Hero Academia: Ahora Es Tu Turno',
'Días perfectos',
'La garra de hierro',
'Star Wars Episodio 1: La amenaza fantasma (1999) (re:2024)',
'One Direction: This Is Us (2013) (re:2024)',
'El club de los vándalos',
'Parpadea dos veces',
'Borderlands',
'Cómplices del engaño',
'La leyenda del dragon',
'Hybe Cine Fest in Latam 2024',
'Paranoia',
'La viuda de Clicquot',
'Niko: La Aventura de las narices frias',
'Godless: The Eastfield Exorcism',
'Sting Araña Asesina',
'Bagman: El Espiritu del Mal',
'La Favorita del Rey',
'Blue Lock: Episodio Nagi',
'Atentado en Madrid',
'El Jockey',
'The Chosen: Temporada 4 - capitulos 7 y 8',
'Kill: Masacre en el tren',
'Guadalupe: Madre de la humanidad',
'No Lo Abras',
'Contra todos',
'Queer',
'RM: Right People Wrong Place',
'Ghost: Rite here rite now',
'Cabrini',
'Siempre habrá un mañana',
'Bienvenidos al Paraíso',
'Harry Potter y el Prisionero de Azkaban (re: 2024)',
'Un Hombre Diferente',
'Después de la muerte',
'Deep weeb show Mortal',
'Zak y Wowo',
'La Secta',
'La conversión',
'Dobles de acción',
'Gurren Lagann the movie: the lights in the sky are stars',
'La Renga: Totalmente Poseídos',
'La Cocina',
'El gran cambio',
'Susurros Mortales 2',
'Batman (1989) (re:2024)',
'Donde Habita el Diablo',
'Una Boda y Otros Desastres',
'El reino animal',
'Queen Rock Montreal',
'Crónica Sangrienta',
'Operacion Zombie: Resurreccion',
'Hijo del Diablo',
'Beetlejuice (1988) (re:2024)',
'Mubi Fest (2024)',
'La matriarca',
'El viento que arrasa',
'Gundam Seed Freedom',
'Atrapa almas',
'El teorema de Marguerite',
'Daaaaaali!',
'La Vida de Jagna',
'Nacimiento',
'Seventeen Right Here World Tour in Japan: Live Viewing',
'Vera y el placer de los otros',
'Muti: Rituales Mortales',
'El viaje soñado',
'Valeria viene a casarse',
'Lo que quisimos ser',
'Miranda de viernes a lunes',
'Whitnet Houston The Concert',
'Alter ego',
'Paisaje',
'Sin códigos',
'Los Tonos Mayores',
'Un Pájaro Azul',
'Delirio',
'Tormenta de Fuego: Incendios en la Patagonia',
'Estela una vida',
'El Eco',
'La Suprema',
'Runner',
'El Tío',
'The quiet girl',
'El rectángulo de Angeles',
'Nos volveremos a encontrar un día de sol',
'Abandono de cargo',
'Al impenetrable',
'42 Reinicio',
'Yakuman: Hacia donde van las aguas',
'La Forja',
'Transformers uno',
'Atrapados en lo profundo',
'Sobrevivientes después del terremoto',
'Spy X Family Código: Blanco',
'Harold y su crayón mágico',
'Hereje',
'Back to black',
'Red',
'Mascotas en apuros',
'El Asesino del Juego de Citas',
'Los que se quedan',
'Secretos de un escándalo',
'El Juego de la Muerte',
'Herencia Siniestra',
'El Señor de los Anillos: La Guerra de los Rohirrim',
'El bufón',
'Vermin: La Plaga',
'Alas Blancas',
'Golpe de suerte en Paris',
'Monkey man',
'The Chosen Temporada 4: Capítulos 5 y 6',
'Siempre juntos',
'Criaturas: Lineas de Extincion',
'Capitán Avispa',
'Sleep: El mal no duerme',
'Una jungla de locura 2',
'El Sabor de la Vida',
'Uranus 2324',
'El color púrpura',
'Nueve Reinas (re:2024)',
'Dogman',
'Culpa cero',
'Good Boy',
'La maldición de cenicienta',
'El bastardo',
'Yo Capitán',
'El último escape',
'Seventeen Tour Follow Agai to Cinemas',
'Un panda en Africa',
'Boonie Bears: Código Guardian',
'Tuesday: El último abrazo',
'Mavka: La guardiana del bosque',
'Dan Da Dan: Primer Encuentro',
'Stop Making Sense',
'Club zero',
'Dalia y el libro rojo',
'Uma & Haggen: Mitos',
'Aespa: World Tour in Cinemas',
'El reino de Kensuke',
'Volver al Futuro (1985) (re:2024)',
'Bernardette La mujer del presidente',
'Paul McCartney and Wings - One Hand Clapping',
'Todos somos extraños',
'Lo mejor esta por venir',
'El Extraño Mundo de Jack (Re: 2024)',
'Plan de retiro',
'Amor sin tiempo',
'Azucar y Estrellas',
'Asesino serial',
'La Quimera',
'Entre Nosotros',
'Festival Internacional de Cine de Mar del Plata 2024',
'Malta',
'Gladiador (re:2024)',
'Alemania',
'Testamento',
'El jardín del deseo',
'Matrix (25° Aniversario)',
'El Libro de las soluciones',
'Festival Buenos Aires Rojo Sangre (2024)',
'Las corredoras',
'Como ser millonario antes que muera la abuela',
'Festival de Cine Frances 2024',
'La Inocencia',
'Despierta mamá',
'Pet Shop Boys dreamworld: The Greatest hits live at the Royal Arena Copenhagen',
'Transformers: 40 Aniversario',
'Pearl Jam - Dark Matter - Global Theatrical Experience - One Night Only',
'Rita',
'Superman 1978 (re:2024)',
'Anora',
'Corre',
'La Inmensidad',
'Taeyong: ty track in Cinemas',
'Carnaval',
'Secretos oscuros',
'Vampiro al Rescate',
'Reinas',
'19° Festival Internacional de Cine Judío 2024',
'Como el mar',
'Ciclo Anime 2024',
'Cine Club (2024)',
'Coldplay: Moon Music',
'Festival de Cine Aleman 2024',
'No Quiero Ser Polvo',
'La Conspiración del Diablo',
'Semana Cine Italiano 2024',
'Faq 7',
'Nunca es tarde para amar',
'Detras de la verdad',
'Transmitzvah',
'Crónicas de una santa errante',
'El escuerzo',
'El Lago de los Cisnes',
'Traslados',
'Monumental Decadentes',
'Simon',
'Blur: Live at Wembley Stadium',
'Vladimir',
'Una jirafa en el balcón',
'Simon de la montaña',
'La Luz que Imaginamos',
'Love Life Lo que fuimos vive siempre',
'El Llanto',
'Nada por perder',
'La noche que luche contra dios',
'No hay osos',
'Werther: el musical',
'Rapto',
'La vida de Jagna',
'San Pugliese',
'Historias Invisibles',
'9 Semanas y media (re:2024)',
'Tres Hermanos',
'Choolate Para tres',
'El Bello Verano',
'Puan',
'Salvajes',
'Continente',
'Una sola primavera',
'Alma y Oskar',
'La sociedad del afecto',
'Estepa',
'Clara se pierde en el bosque',
'Tiempo de pagar',
'Bajo Naranja',
'Pesadilla en la calle Elm',
'18° Festival Internacional de Cine Judío 2024',
'Corresponsal',
'Luces azules',
'Usher: Rendezvous in Paris',
'Música',
'Nunca es Tarde',
'Las Bestias',
'Maria',
'Elda y los monstruos',
'Nefarious',
'Lo Que No Vemos',
'Revelar: Indicios de Identidad',
'Los Afectos',
'Faro',
'La Montaña',
'Leyenda Feroz',
'El Señor de las Ballenas',
'Recursos Humanos',
'Paloma',
'El placer es mío',
'Sin Salida',
'Las Fieras',
'La Practica',
'Romina Smile',
'Tokyo Shaking',
'La noche adentro',
'El sonido de antes',
'El Santo',
'Búfalo',
'Ciclo Noir Criollo',
'El Agujerito',
'Las leguas',
'Martín García',
'Recuerdos del Mal',
'La gruta continua',
'Continuará',
'La Película',
'Literal',
'Misericordia',
'El nuevo novio de Lucía',
'La Habana de Fito',
'Dormir con los Ojos Abiertos',
'Andrea Bocelli 30: La Celebracion',
'Trapezium',
'Un silencio',
'El agrónomo',
'Hate to Love: Nickelback',
'Noches de terror 3',
'Habitación 404',
'Auxilio',
'Las Cosas Indefinidas',
'WOS: Descartable en vivo Racing Club',
'Ciudad oculta',
'Sublime',
'42',
'La culpa de nada',
'Mixtape La Pampa',
'Berta y Pablo',
'Seret - Festival Internacional de Cine Judio 2024',
'Settembre',
'Realizaciones del Posgrado Documental 2024',
'Semana de Cine del Festival de Cannes 2024',
'Weekend',
'Blues de la civilización',
'Naufragios',
'Reflejado',
'Festival de Cine Inusual 2024',
'Epa Cine 2024',
'Un hombre que escribe',
'Las amantes astronautas',
'Jinetes de Roca',
'Paisaje épico',
'Una Mirada Honesta',
'Las Fronteras del Tiempo',
'Festival Latinoamericano Tucumán Cine Gerardo Vallejo 2024',
'Ciclo de Cine Iberoamericano 2024',
'Como si fueramos solo amigos',
'Orégano: la Familia Fracaso',
'El Auge de lo Humano 3',
'Exilio',
'Los Últimos',
'La Historia de mi mujer',
'Temporal',
'Felipe',
'Billinghurst',
'Midsommar',
'Así baila mi Perú',
'A Traves de la Tierra',
'Lagunas',
'Tres extraños',
'No corre el viento',
'Historia de Dos guerreros',
'BitBang: 10° Festival Internacional de Cine de Animación',
'Como copiarse en un examen',
'Semana de Cine en Salta 2024',
'Perdidos en la Noche',
'La noche del crimen',
'Mala Reputación',
'Tras las huellas de Mengele',
'Invisibles',
'Extravagantes',
'Carnaval de las almas (re:2024)',
'El fantasma de la familia rampante',
'Cabeza parlante boca muda',
'Caballos',
'Historias Breves 21',
'Intemperie',
'Oid mortales',
'Los jóvenes amantes',
'Durazno sangrando',
'Ziline: Entre el mar y la montaña',
'Látex rojo',
'Ciclo Soy lo que soy',
'El proceso',
'Venganza de un vampiro',
'Quémenlos',
'Horizonte',
'Recuerda',
'La creación',
'YVY Tierra',
'Hamlet',
'La película de Bañez',
'Las ausencias',
'Carmelo Saitta. Collage 1944',
'Esa casa amarilla',
'La otra memoria del mundo',
'Mujeres del Sahara libre',
'Esperando al mar',
'El verano más largo del mundo',
'Corazón Embalsamado',
'El Realismo Socialista',
'Salam',
'Clásicos de todas las épocas',
'Fuego sin Ley',
'Otra Piel',
'El Criar del Rio',
'Festival Cine y Diversidad Sexual 2024',
'Ruletka',
'El exito del amor',
'El Santo Marginal',
'El Dragón',
'Generación tango',
'Camuflaje',
'La Invitada',
'Poltergeist (re:2024)',
'Retrospectiva John Carpenter',
'Alien: el octavo pasajero (re-2024)',
'Fragmentada',
'Lago escondido: la soberanía en juego',
'Tras un manto de neblinas',
'Amor Urgente',
'Sin Clemencia',
'Talia',
'Mi Villano Favorito',
'Golden Boyz: Tesoros Invisibles',
'Oda Amarilla',
'Reflejo de un pescador',
'Darlo Todo',
'Rapado (Re 2024)',
'Colmillos',
'Festival Internacional de Cine Sordo de Argentina',
'Recuerdos Mortales',
'El hombre de los sueños',
'Harry Potter y el Prisionero de Azkaban (Re: 2024)',
'Straight',
'Caminos cruzados',
'Here',
'El mal no existe',
'Godzilla Minus One',
'Jaque Mate',
'Bafici 2024',
'Linda',
'Festival Entra al Mas Alla',
'Robotia',
'Cielo Rojo',
'Iluminados',
'Hijos',
'Repulsión (re:2024)',
'Los Domingos Mueren Mas Personas',
'Tears For Fears Live (A Tipping Point Film)',
'El primer día de mi vida',
'Maestro (s)',
'El divino Zamora',
'Rascal does not dream of a knapsack kid',
'El Castigo',
'Fantasma',
'Crowrã. La flor del burití',
'Placebo: This Search For Meaning',
'Hay Una Puerta Ahí',
'Norita',
'Silvia Prieto 25 Aniversario',
'Recuerdos de Paris',
'Las Dos Mariette',
'Las ocho montañas',
'Zapatos Rojos',
'Eureka',
'La Sudestada',
'Brother',
'Ciclo Etiqueta negra (2024)',
'Transe',
'Tuve el corazón',
'León',
'Los Bilbao',
'Julia no te Cases',
'Dahomey',
'Metok',
'Entremedio',
'Salidos de la Salamanca. U viaje hacia la chacarera',
'Un Buen Padre',
'Después de un buen día',
'Italpark',
'Increible pero cierto',
'Trayetorias Gaumont: Alejandro Doria',
'La sombra del comandante',
'Banff Mountain Film Festival World Tour (2024)',
'Hijos de la revolución',
'Los Justos',
'Nosferatu',
'Adolfo',
'No esperes demasiado del fin del mundo',
'La Mujer Hormiga',
'La Ruptura',
'Bajo el sol del rocanrol',
'Ciclo Sandro en Primavera',
'Reas',
'BamvFest 2024',
'Rebelión',
'Enerc se proyecta 2024',
'Marzo',
'Semana del Tango',
'La estrella que perdí',
'Fighter',
'Norte',
'Fumar provoca tos',
'Cross Dreamers',
'13° edición del Festival Latinoamericano Cortópolis 2024.',
'El viaje de Alba',
'Hermanas',
'Corsage. La emperatriz rebelde',
'Semana de Cine Portugues 2024',
'Virgen Rosa',
'Vainilla',
'Cantando las raíces',
'Mover (lo que no se ve)',
'Dueto',
'Las voces de Pablo',
'La Terminal',
'Feed Dog Argentina Festival Internacional de Cine Documental sobre Moda',
'Algún día en algún lugar',
'El amigo visible',
'Sing Sing',
'Festival Cine a la Vista 2024',
'Antes del Adios',
'Desiderio: Reflexiones sobre un autorretrato',
'El campo en mí',
'Votos',
'Otra pelicula maldita',
'Mi padre y yo',
'Sweeney Todd: El Barbero Demoníaco',
'Mejunje',
'Ciclo Cine y Libros',
'Nicola',
'La memoria que habitamos',
'Diario de una Pasión',
'Limbo alucinante',
'Edgardo Cozarinsky - In Memoriam',
'El Ascenso',
'El mal absoluto',
'El Mar Invisible',
'Moebius (Re 1996)',
'Homofobia',
'Ciego',
'Popular tradición de esta tierra',
'Tempus Fugit',
'La tiendita del terror (re:1960)',
'El Nacional',
'Historia Universal',
'Procopiuk',
'Horizontes cercanos de la naturaleza',
'Antarktis',
'Pussy Cake',
'Amor y cine',
'El cine por mujeres (2024)',
'El último Guerrero',
'Las Preñadas',
'Conclave',
'Luis Benítez y el mundo de la poesía',
'ROH: Manon',
'Ifwalapej',
'Prode',
'Retrospectiva Eduardo Teddy Williams',
'Doctor Cerebro',
'Imprenteros',
'El regreso de los muertos vivos (re-2024)',
'Trenque Lauquen',
'Langosta',
'¡Obreros!',
'Las hijas',
'Astérix & Obélix: Mission Cleopatra',
'Ventajas de viajar en tren',
'La Dama',
'Ciclo Genero DAC',
'Ciclón fantasma',
'Picado fino (Re 2024)',
'Camino a Mailin',
'Quiero Volverme Tiempo',
'Ciclo Delon Toujours',
'Ciclo Cine Basko 2024',
'Los impactados',
'Cor',
'Hombre muerto',
'Quizás es cierto lo que dicen de nosotras',
'Sara Facio: Haber estado ahí',
'La estrella azul',
'Lo que queda',
'El testigo (Conversaciones con Osvaldo Bayer)',
'Hemshej',
'Semillas que caen lejos de sus raices',
'Gaucho Gaucho',
'Tótem',
'Caminemos Valentina',
'Llullaillaco en la piel del pasado',
'Las Noches son de los Monstruos',
'Scafati. Palabra pintada',
'Tiempo largo y jodido ¿Que queres que te diga?',
'La Rosa Negra: herencia ancestral',
'Robots',
'Chagas: Orquesta invisible',
'La vida que siempre soñaste',
'Mamma Mía!',
'(El Indio...)',
'Duele',
'Cambio cambio',
'Ciclo Seguir La huella',
'Si yo fuera el invierno mismo',
'Blondi',
'Cielo rojo: gigantes de metal',
'La Busqueda: el documental mas humano sobre la vida extraterrestre']

# COMMAND ----------

imdb_mask = pd.DataFrame(imdb_mask)


# COMMAND ----------

imdb_mask.rename(columns={0:'FULL_TITLE'}, inplace=True)

# COMMAND ----------

imdb_mask['IMDB_ID']=''

# COMMAND ----------

imdb_mask

# COMMAND ----------

# First with DuckDuckGo
mapping_data =imdb_mask 
mapping_data.loc[imdb_mask, 'IMDB_ID'] = mapping_data.loc[imdb_mask, ['CONTENT_CLASS', 'FULL_TITLE', 'RELEASE_YEAR', 'MAIN_ACTOR']].progress_apply(imdb_search_ddgs, axis=1)

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
