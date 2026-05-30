{{ config(materialized='table') }}

select distinct
    num_poste,
    nom_usuel,
    lat as latitude,
    lon as longitude
from read_parquet({{ source("meteo_france_quot", "rr_t_vent") }})