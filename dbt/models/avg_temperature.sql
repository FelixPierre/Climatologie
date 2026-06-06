{{ config(materialized='table') }}

select
    num_poste,
    -- nom_usuel,
    date_mesure,
    annee_mesure,
    mois_mesure,
    jour_mesure,
    tmin,
    tmax,
    tavg,
    tmin_max_avg,
    avg(tmin_max_avg) over (
        partition by num_poste, mois_mesure, jour_mesure
        order by annee_mesure asc
        range between 10 preceding and current row
        exclude current row
    ) as tmobile
from {{ ref("temperature") }}
