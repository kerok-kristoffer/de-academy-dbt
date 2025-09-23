{% macro load_providerinfo_bronze() %}
  {% set cols = [
    'CMS Certification Number (CCN)', 'Provider Name', 'Provider Address',
    'City/Town', 'State', 'ZIP Code', 'Telephone Number',
    'Provider SSA County Code', 'County/Parish', 'Ownership Type',
    'Number of Citations from Infection Control Inspections',
    'Number of Fines', 'Total Amount of Fines in Dollars',
    'Number of Payment Denials', 'Total Number of Penalties', 'Location',
    'Latitude', 'Longitude', 'Geocoding Footnote', 'Processing Date'
  ] %}
  {{ copy_csv_to_bronze(
     target_table = 'healthcare_db.bronze.providerinfo_raw',
     stage_name   = 'healthcare_db.bronze.gdrive_landing_stage',
     file_pattern = '.*NH_ProviderInfo_.*\\.csv',
     columns      = cols
  ) }}
{% endmacro %}
