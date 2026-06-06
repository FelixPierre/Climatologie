{{ config(materialized='table') }}

select
    num_poste,
    -- nom_usuel,
    annee_mesure,
    mois_mesure,
    avg(tmin) as t_min_avg,
    avg(tmax) as t_max_avg,
    avg(tmin_max_avg) as t_min_max_avg,
    avg(tavg) as t_avg,
    avg(tmobile) as t_mobile_avg
from {{ ref("avg_temperature") }}
group by num_poste, annee_mesure, mois_mesure
