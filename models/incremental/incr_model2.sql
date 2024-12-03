{{ config(
    materialized='incremental',
    unique_key='id'
) }}

{% set inf_date = "'2999-12-31'" %}

with t0 as (
    select
        id,
        value,
        is_deleted,
        current_timestamp - interval '5 days' as updated_at
    from {{ ref('stg_is_deleted_0') }}
),

t1 as (
    select
        id,
        value,
        is_deleted,
        current_timestamp as updated_at
    from {{ ref('stg_is_deleted_1') }}
),

identical_records as (
    -- Record identici tra t0 e t1 con flag N, senza duplicati
    select distinct
        t0.id,
        t0.value,
        t0.is_deleted,
        t0.updated_at as start_date,
        cast({{ inf_date }} as date) as last_date
    from t0
    inner join t1
        on t0.id = t1.id
        and t0.value = t1.value
        and t0.is_deleted = t1.is_deleted
        and t0.is_deleted = 'N'
        -- Escludiamo i record che cambiano a 'Y'
        where not exists (
            select 1
            from t1 as sub_t1
            where sub_t1.id = t0.id
              and sub_t1.is_deleted = 'Y'
        )
),

changed_to_deleted as (
    -- Prima parte: Record che cambiano da 'N' a 'Y'
    select distinct
        t0.id,
        t0.value,
        'N' as is_deleted,
        t0.updated_at as start_date,
        t1.updated_at as last_date
    from t0
    inner join t1
        on t0.id = t1.id
        and t0.is_deleted = 'N'
        and t1.is_deleted = 'Y'

    union all

    -- Seconda parte: Record aggiornati con stato 'Y'
    select distinct
        t1.id,
        t1.value,
        'Y' as is_deleted,
        t1.updated_at as start_date,
        cast({{ inf_date }} as date) as last_date
    from t0
    inner join t1
        on t0.id = t1.id
        and t0.is_deleted = 'N'
        and t1.is_deleted = 'Y'
),

new_records as (
    -- Record nuovi presenti solo nella seconda tabella
    select distinct
        t1.id,
        t1.value,
        t1.is_deleted,
        t1.updated_at as start_date,
        cast({{ inf_date }} as date) as last_date
    from t1
    left join t0
        on t0.id = t1.id
    where t0.id is null
),

final_table as (
    select * from identical_records
    union all
    select * from changed_to_deleted
    union all
    select * from new_records

    /*
    {% if is_incremental() %}
    where start_date > (select max(updated_at) 
                        from {{ this }})
    {% endif %}
    */
)

select * from final_table
order by id