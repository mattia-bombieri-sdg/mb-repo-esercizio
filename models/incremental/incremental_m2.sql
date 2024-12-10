{{ config(
    materialized='incremental',
    unique_key=['id', 'valid_from'],
    incremental_strategy='merge',
    post_hook= "update {{this}} set is_active = 'Y' where valid_to = '2999-12-31 00:00:00.000'"
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

delta as (
    select
        new.id as id,
        new.value as value,
        new.is_deleted as is_deleted,
        current_timestamp as valid_from,
        {% if not is_incremental() %}
            cast('2999-12-31' as timestamp) as valid_to
        {% else %}
            case 
                when new.is_deleted = 'Y' then current_timestamp
                else cast('2999-12-31' as timestamp)
            end as valid_to
        {% endif %}
    from 
        {% if not is_incremental() %}
            t0_source as new
        {% else %}
            t1_source as new
        {% endif %}
        {% if is_incremental() %}
        left outer join {{ this }} as old
            on new.id = old.id
        where 
            old.id is null                         -- id nuovo
            or new.value != old.value              -- value cambiato
            or new.is_deleted != old.is_deleted    -- is_deleted cambiato
        {% endif %}
),

old as (
    select
        *
    from {{this}} as old
    qualify row_number() over(partition by id order by valid_from desc) = 1
),

old_join_delta as (
    select
        old.id,
        old.value,
        old.is_deleted,
        old.valid_from,
        delta.valid_from as valid_to
    from old
    inner join delta
        on old.id = delta.id
),

final as (
    select * from delta
    {% if is_incremental() %}
    union all
    select * from old_join_delta
    {% endif %}
)

select *, '' as is_active from final