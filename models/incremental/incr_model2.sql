{{ config(
    materialized='incremental',
    unique_key='concat(id, value, is_deleted)'
) }}

{% set inf_date = "'2999-12-31'" %}

with t0 as (
    select
        id,
        value,
        is_deleted,
        updated_at
    from {{ ref('stg_is_deleted_0') }}
),

t1 as (
    select
        id,
        value,
        is_deleted,
        updated_at
    from {{ ref('stg_is_deleted_1') }}
),

-- Record identici tra t0 e t1 con flag N, senza duplicati
identical_records as (
    select distinct
        t0.id,
        t0.value,
        t0.is_deleted,
        t0.updated_at as start_date,
        cast({{ inf_date }} as date) as last_date
    from t0
    inner join t1
        on t0.id = t1.id and t0.value = t1.value
    where t0.is_deleted = 'N' and t1.is_deleted = 'N'
),

-- Record che tra t0 e t1 cambiano il flag da N a Y, quindi sono duplicati
changed_to_deleted as (
    -- Record che cambiano da 'N' a 'Y', quindi hanno inizio e una fine
    select distinct
        t0.id,
        t0.value,
        t0.is_deleted,
        t0.updated_at as start_date,
        t1.updated_at as last_date
    from t0
    inner join t1
        on t0.id = t1.id and t0.value = t1.value
    where t0.is_deleted = 'N' and t1.is_deleted = 'Y'

    union all

    -- Record aggiornati con stato 'Y', quindi che hanno solo un inizio
    select distinct
        t1.id,
        t1.value,
        t1.is_deleted,
        t1.updated_at as start_date,
        cast({{ inf_date }} as date) as last_date
    from t1
    inner join t0
        on t0.id = t1.id and t0.value = t1.value
    where t0.is_deleted = 'N' and t1.is_deleted = 'Y'
),

-- Record nuovi presenti solo nella seconda tabella con flag N, senza suplicati
new_records as (
    select distinct
        t1.id,
        t1.value,
        t1.is_deleted,
        t1.updated_at as start_date,
        cast({{ inf_date }} as date) as last_date
    from t1
    left join t0
        on t0.id = t1.id and t0.value = t1.value
    where t0.id is null
),

final_table as (
    select * from identical_records
    union all
    select * from changed_to_deleted
    union all
    select * from new_records

    {% if is_incremental() %}
    where start_date >= (select max(start_date) 
                        from {{ this }})
    {% endif %}
)

select * from final_table
order by id