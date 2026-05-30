{{ config(materialized='table') }}

select
    num_poste,
    nom_usuel,
    lat as latitude,
    lon as longitude,
    aaaammjj as date_mesure,
    tn as min,
    tx as max,
    tm as avg,
    tntxm as min_max_avg,
    tampli as amplitude
from read_parquet({{ source("meteo_france_quot", "rr_t_vent") }})
where tn is not null -- filter missing temperatures