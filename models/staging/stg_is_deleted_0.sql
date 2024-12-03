with source as (
    select
        id,
        value,
        is_deleted
    from {{ source('is_deleted', 'data_t0') }}
)
select * from source
order by id