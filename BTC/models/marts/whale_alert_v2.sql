WITH WHALES as (
    select
        output_address,
        sum(output_value) as total_sent,
        count(*) as tx_count

    from {{ ref('stg_btc_transactions')}}

    where output_value > 5

    group by output_address
    order by total_sent desc
)
select
    CAST('{{ invocation_id }}' as string) as invocation_id,
    w.output_address,
    w.total_sent,
    w.tx_count
from WHALES w
order by total_sent desc
