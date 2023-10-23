-- Create the raw_census2 table by referencing the `census2` table
with raw_census2 as (
    select
        lga_code_2016,
        median_age_persons,
        median_mortgage_repay_monthly,
        median_tot_prsnl_inc_weekly,
        median_rent_weekly,
        median_tot_fam_inc_weekly,
        average_num_psns_per_bedroom,
        median_tot_hhd_inc_weekly,
        average_household_size
    from {{ source('raw','census2') }}
)

select * from raw_census2