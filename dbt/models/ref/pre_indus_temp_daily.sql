{{ config(materialized='table') }}

select
    num_poste,
    mois_mesure,
    jour_mesure,
    avg(tmin) as t_min,
    avg(tmax) as t_max,
    avg(tavg) as t_avg
from {{ ref("temperature") }}
where annee_mesure >= 1850
    and annee_mesure <= 1900
group by num_poste, mois_mesure, jour_mesure
