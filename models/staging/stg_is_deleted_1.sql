with source as (
    select
        *,
        current_timestamp as updated_at
    from {{ source('is_deleted', 'data_t1') }}
)
select * from source
order by id