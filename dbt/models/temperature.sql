{{ config(materialized='table') }}

select
    num_poste,
    nom_usuel,
    -- lat as latitude,
    -- lon as longitude,
    strptime(aaaammjj::varchar, '%Y%m%d') as date_mesure,
    date_part('year', date_mesure) as annee_mesure,
    date_part('month', date_mesure) as mois_mesure,
    date_part('day', date_mesure) as jour_mesure,
    tn as tmin,
    tx as tmax,
    tm as tavg,
    tntxm as tmin_max_avg,
    tampli as amplitude
from read_parquet({{ source("meteo_france_quot", "rr_t_vent") }})
where tn is not null -- filter missing temperatures