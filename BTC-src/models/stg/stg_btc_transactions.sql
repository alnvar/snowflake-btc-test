{{ config(materialized='ephemeral')}} --This basically creates an ecapsulation for CTEs

select
*
from {{ ref('stg_btc_outputs')}}

where coalesce(is_coinbase,false) = false
