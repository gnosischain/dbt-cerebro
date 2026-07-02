
    
    

with all_values as (

    select
        user_segment as value_field,
        count(*) as n_records

    from `dbt`.`int_execution_gnosis_app_gt_user_dim`
    group by user_segment

)

select *
from all_values
where value_field not in (
    'registry_only_no_avatar','avatar_blank_type','Signup','RegisterHuman','Unknown','Unclaimed','OrganizationSignup','Invite','Migrating','RegisterOrganization','RegisterGroup'
)


