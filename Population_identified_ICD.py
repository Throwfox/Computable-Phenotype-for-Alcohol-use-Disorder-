import pandas as pd
import duckdb

# Concept IDs 
AUD_ICD = [
    # ICD-9 Codes
    "303.0", "303.01", "303.02", "303.03", "303.00", "303", "303.9", "303.91", "303.92", "303.93", "303.90",
    "305.0", "305.01", "305.02", "305.03", "305.00",
    
    # ICD-10 Codes
    "F10.1", "F10.180", "F10.14", "F10.15", "F10.150", "F10.151", "F10.159", "F10.181", "F10.182", "F10.12",
    "F10.121", "F10.120", "F10.129", "F10.188", "F10.18", "F10.19", "F10.131", "F10.132", "F10.130", "F10.139",
    "F10.11", "F10.10", "F10.13", "F10.2", "F10.280", "F10.24", "F10.26", "F10.27", "F10.25", "F10.250",
    "F10.251", "F10.259", "F10.281", "F10.282", "F10.22", "F10.221", "F10.220", "F10.229", "F10.288", "F10.28",
    "F10.29", "F10.23", "F10.231", "F10.232", "F10.230", "F10.239", "F10.21", "F10.20"
]


AUD_ICD = [code.replace('.', '') for code in AUD_ICD]


data_path= '/media/volume/GLP/RDRP_6263_AUD/'
results_path= 'results/'

# Initialize DuckDB connection
print('Initializing DuckDB connection...')
con = duckdb.connect(database=':memory:')

# Create AUD_ICD list as SQL-compatible string
aud_icd_sql = "', '".join(AUD_ICD)

print('Loading and processing data with DuckDB (efficient for large files)...')
# Use DuckDB to query CSV directly without loading everything into memory
# This is much faster than pandas for large files!
query = f"""
WITH processed_conditions AS (
    SELECT 
        person_id,
        condition_source_value,
        visit_occurrence_id,
        -- Extract ICD code: remove prefix before ^^, remove trailing ^, remove dots
        REPLACE(RTRIM(SPLIT_PART(condition_source_value, '^^', 2), '^'), '.', '') AS processed_code
    FROM read_csv_auto('{data_path}r6263_condition_occurrence.csv')
    WHERE condition_source_value IS NOT NULL
)
SELECT 
    person_id,
    condition_source_value,
    visit_occurrence_id,
    processed_code
FROM processed_conditions
WHERE processed_code IN ('{aud_icd_sql}')
"""

print('Executing query to filter AUD ICD codes...')
aud_occurrence = con.execute(query).df()

# Show sample of matched codes
print(f"\n{'='*80}")
print(f"Found {len(aud_occurrence)} AUD ICD code records")
if len(aud_occurrence) > 0:
    print(f"\nSample of processed codes (first 10):")
    print(aud_occurrence[['condition_source_value', 'processed_code']].head(10))

# Merge with visit information (load CSV for visit data)
print('\nMerging with visit occurrence data...')
visit_query = f"""
SELECT 
    visit_occurrence_id,
    visit_concept_id
FROM read_csv_auto('{data_path}r6263_visit_occurrence.csv')
"""
all_visit = con.execute(visit_query).df()
aud_occurrence = aud_occurrence.merge(all_visit, on='visit_occurrence_id', how='left')

# Drop the processed_code column as we don't need it anymore
aud_occurrence = aud_occurrence.drop(columns=['processed_code'])

# Save the result to a CSV file (optional)
def calculate_visit_counts(aud_occurrence):
    """
    Calculate inpatient and outpatient visit counts for each patient.
    Based on actual visit_concept_id values in the dataset:
    - 9201: Inpatient Visit
    - 9202: Outpatient Visit  
    - 9203: Emergency Room Visit (counted as inpatient)
    """
    # Map visit_concept_id to categories
    aud_occurrence['visit_category'] = aud_occurrence['visit_concept_id'].map(
        lambda x: 'inpatient' if x in [9201] else 'outpatient' if x in [9202] else None
    )

    # Filter out rows with undefined visit categories
    aud_occurrence = aud_occurrence.dropna(subset=['visit_category'])

    # Count occurrences by category
    visit_counts = aud_occurrence.groupby(['person_id', 'visit_category']).size().unstack(fill_value=0)
    visit_counts = visit_counts.rename(columns={'inpatient': 'inpatient_count', 'outpatient': 'outpatient_count'})

    # Ensure both columns exist even if one type of visit is missing
    if 'inpatient_count' not in visit_counts:
        visit_counts['inpatient_count'] = 0
    if 'outpatient_count' not in visit_counts:
        visit_counts['outpatient_count'] = 0

    visit_counts = visit_counts.reset_index()

    return visit_counts[['person_id', 'inpatient_count', 'outpatient_count']]

# Actual visit_concept_id values in this dataset:
# 9201: Inpatient Visit (counted as inpatient)
# 9202: Outpatient Visit (counted as outpatient)
# 9203: Emergency Room Visit (counted as inpatient)
# 0: Undefined/Missing (excluded from analysis)
#
# Note: Standard OMOP codes that are NOT in this dataset:
# 262: Emergency Room and Inpatient Visit
# 8717: Inpatient Hospital
# 8756: Outpatient Hospital

# Display visit type distribution
print("Visit type distribution:")
print(aud_occurrence['visit_concept_id'].value_counts())
print(f"\nTotal unique patients with AUD ICD codes: {aud_occurrence['person_id'].nunique()}")

# Calculate visit counts by patient
patient_counts = calculate_visit_counts(aud_occurrence)
def filter_patients_by_visits(patient_counts):
    """
    Filter patients with at least one inpatient visit or at least two outpatient visits.
    """
    filtered_patients = patient_counts[
        (patient_counts['inpatient_count'] >= 1) | (patient_counts['outpatient_count'] >= 2)
    ]
    return filtered_patients
filtered_patients=filter_patients_by_visits(patient_counts)
filtered_patients.to_csv(results_path+'aud_patients_ICD_AUD_rule.csv',index=False)

# Close DuckDB connection
con.close()

print("Extraction complete. Results saved")
print(f"Final results: {len(filtered_patients)} patients meet the criteria (inpatient>=1 or outpatient>=2)")