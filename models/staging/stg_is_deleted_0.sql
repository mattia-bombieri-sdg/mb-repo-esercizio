with source as (
    select
        *
    from {{ source('is_deleted', 'data_t0') }}
)
select * from source