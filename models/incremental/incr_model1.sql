{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with t1 as (
    select
        id,
        value,
        is_deleted
    from {{ ref('stg_is_deleted_0') }}
),

t2 as (
    select
        id,
        value,
        is_deleted
    from {{ ref('stg_is_deleted_1') }}
),

t1_not_in_t2 as (
    select
        t1.id,
        t1.value,
        t1.is_deleted
    from t1
    left join t2
        on t1.id = t2.id
    where t2.id is null
),

t2_with_deleted as (
    select
        t2.id,
        t2.value,
        t2.is_deleted
    from t2
    where t2.is_deleted = 'Y'
),

new_records_in_t2 as (
    select
        t2.id,
        t2.value,
        t2.is_deleted
    from t2
    left join t1
        on t2.id = t1.id
    where t1.id is null
)

-- Uniamo tutte le parti in un'unica selezione
select * from t1_not_in_t2
union all
select * from t2_with_deleted
union all
select * from new_records_in_t2