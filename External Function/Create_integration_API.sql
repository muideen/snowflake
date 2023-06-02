create role integration_manager;
grant CREATE INTEGRATION ON ACCOUNT to role integration_manager;
grant role integration_manager to user Muideen;
use role integration_manager;

--Create Azure Integration API
create or replace api integration Azure_func_integration
    api_provider = azure_api_management
    azure_tenant_id = '8291ea07-ed46-498c-8427-7e98041e8691'
    azure_ad_application_id = 'df6baab0-7951-4021-9fb4-02fba9a35cc0'
    api_allowed_prefixes = ('https://sf-api-manager.azure-api.net/sf-ext-func')
    enabled = true;