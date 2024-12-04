{{ config(
    materialized='incremental',
    unique_key='id'
) }}

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

-- Record che non sono stati modificati (flag N sia in t0 che in t1)
-- La data di aggiornamento rimane quella della tabella t0
t1_with_not_deleted_from_t0 as (
    select
        t0.id,
        t0.value,
        t0.is_deleted,
        t0.updated_at
    from t0
    left join t1
        on t0.id = t1.id and t0.value = t1.value
	where t0.is_deleted = 'N' and t1.is_deleted = 'N'
),

-- Record che sono passati da is_deleted='N' a is_deleted='Y'
-- La data di aggiornamento è quella di t1
t1_with_deleted_from_t0 as (
    select
        t1.id,
        t1.value,
        t1.is_deleted,
        t1.updated_at
    from t1
    inner join t0
        on t0.id = t1.id and t0.value = t1.value
	where t0.is_deleted = 'N' and t1.is_deleted = 'Y'
),

-- Nuovi record solo presenti in t1
-- La data di aggiornamento è quella di t1
new_records_in_t1 as (
    select
        t1.id,
        t1.value,
        t1.is_deleted,
        t1.updated_at
    from t1
    left join t0
        on t0.id = t1.id
    where t0.id is null
),

final_table as (
	select * from t1_with_not_deleted_from_t0
		union all
	select * from t1_with_deleted_from_t0
		union all
	select * from new_records_in_t1
	
	{% if is_incremental() %}
		having updated_at > ( select max(updated_at) 
                            from {{ this }} )
	{% endif %}
	
)

select * from final_table
order by id