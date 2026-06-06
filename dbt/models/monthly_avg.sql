{{ config(materialized='table') }}

select
    num_poste,
    -- nom_usuel,
    annee_mesure,
    mois_mesure,
    avg(tmin) over w as t_min_avg,
    avg(tmax) over w as t_max_avg,
    avg(tmobile) over w as t_mobile_avg
from {{ ref("avg_temperature") }}
window w as (
    partition by num_poste, annee_mesure, mois_mesure
)
