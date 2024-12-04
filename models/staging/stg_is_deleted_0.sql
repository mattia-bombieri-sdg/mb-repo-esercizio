with source as (
    select
        id,
        value,
        is_deleted,
        current_timestamp - interval '1 days' as updated_at
    from {{ source('is_deleted', 'data_t0') }}
)
select * from source
order by id