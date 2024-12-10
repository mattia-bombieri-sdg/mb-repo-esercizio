{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
) }}

with
t0_source as (
    select 
        *
    from {{ ref('stg_is_deleted_0') }}
),

t1_source as (
    select 
        *
    from {{ ref('stg_is_deleted_1') }}
),

source as (
    select 
        new.* 
    from
    {% if not is_incremental() %}
        t0_source as new
    {% else %}
        t1_source as new
    {% endif %}

    {% if is_incremental() %}
    left outer join {{ this }} as old
        on new.id = old.id
    where old.id is null
        or new.value != old.value
        or new.is_deleted != old.is_deleted
    {% endif %}
),

final as (
    select 
        *,
        current_timestamp as updated_at
    from source
)

select * from final