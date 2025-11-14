{{ config(materialized='incremental',incremental_strategy='append')}}


with flattened_outputs as (
    select
        tx.hashkey,
        tx.block_number,
        tx.block_timestamp,
        tx.is_coinbase,
        f.value:address::STRING as output_address,
        f.value:value::FLOAT as output_value

    from {{ ref('stg_btc')}} tx,

    LATERAL FLATTEN(input => outputs) f

    WHERE f.value:address is not null

    {% if is_incremental() %}

        and tx.block_timestamp >= (select max(block_timestamp) from {{ this }} )

    {% endif %}

)
select
    hashkey,
    block_number,
    block_timestamp,
    is_coinbase,
    output_address,
    output_value
from flattened_outputs
