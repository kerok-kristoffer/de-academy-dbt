{% macro load_pbj_bronze() %}
  {% set cols = [
    'PROVNUM','PROVNAME','CITY','STATE','COUNTY_NAME','COUNTY_FIPS','CY_Qtr','WorkDate','MDScensus',
    'Hrs_RNDON','Hrs_RNDON_emp','Hrs_RNDON_ctr',
    'Hrs_RNadmin','Hrs_RNadmin_emp','Hrs_RNadmin_ctr',
    'Hrs_RN','Hrs_RN_emp','Hrs_RN_ctr',
    'Hrs_LPNadmin','Hrs_LPNadmin_emp','Hrs_LPNadmin_ctr',
    'Hrs_LPN','Hrs_LPN_emp','Hrs_LPN_ctr',
    'Hrs_CNA','Hrs_CNA_emp','Hrs_CNA_ctr',
    'Hrs_NAtrn','Hrs_NAtrn_emp','Hrs_NAtrn_ctr',
    'Hrs_MedAide','Hrs_MedAide_emp','Hrs_MedAide_ctr'
  ] %}
  {{ copy_csv_to_bronze(
     target_table = 'healthcare_db.bronze.pbj_daily_nurse_staffing_raw',
     stage_name   = 'healthcare_db.bronze.gdrive_landing_stage',
     file_pattern = '.*PBJ_Daily_Nurse_Staffing_.*\\.csv',
     columns      = cols
  ) }}
{% endmacro %}
