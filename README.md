# Develop and Validate a Computable Phenotype for Identifying Alcohol-use Disorder Patients Using Structure and Unstructured EHR Data

This repository contains the code and implementation for developing a computable phenotype to identify patients with Alcohol-use Disorder (AUD) using Electronic Health Record (EHR) data. The approach integrates both structured data (ICD diagnosis codes, medication records) and unstructured data (clinical notes processing via NLP) to improve identification accuracy.

## Repository Structure

The project consists of three data extraction scripts and one analysis notebook:

### 1. Structured Data Extraction
*   **`0drugs.py`**: Identifies patients based on AUD-related medication prescriptions.
    *   Extracts patients with specific drug concept IDs from `drug_exposure` data.
    *   **Rule**: Patients with $\ge$ 1 unique AUD medication.
    *   **Output**: `results/aud_patients_drug_rule.csv`

*   **`0ICD.py`**: Identifies patients based on ICD-9 and ICD-10 diagnosis codes.
    *   Filters `condition_occurrence` data for AUD-specific codes.
    *   Classifies encounters into Inpatient or Outpatient based on `visit_occurrence` data.
    *   **Rule**: Patients with $\ge$ 1 inpatient visit OR $\ge$ 2 outpatient visits with an AUD diagnosis.
    *   **Output**: `results/aud_patients_ICD_AUD_rule.csv`

### 2. Unstructured Data Extraction (NLP)
*   **`0keywords.py`**: Processes clinical notes to identify AUD-related keywords.
    *   Scans text from parquet files using regex patterns defined in `keywords_regex_precise.csv`.
    *   Applies exclusion logic to filter out:
        *   Negations (e.g., "no alcohol use")
        *   Family history context (e.g., "father had AUD")
        *   Legal/administrative text (e.g., "consent form")
    *   **Output**: `results/aud_notes_keywords.csv`

### 3. Phenotype Validation & Analysis
*   **`INPC.ipynb`**: The main analysis notebook.
    *   Loads results from the three extraction scripts.
    *   Performs set operations to analyze the overlap between different data modalities (ICD vs. Drugs vs. NLP).
    *   Evaluates various phenotype definitions (e.g., "Structure data only" vs. "Structure + Unstructured").
    *   Generates statistics on patient counts for each phenotype definition.

## Prerequisites

The code requires Python and the following libraries:
*   `pandas`
*   `duckdb` (for efficient querying of large CSV files)
*   `tqdm` (for progress bars)
*   `numpy`

Install dependencies via pip:
```bash
pip install pandas duckdb tqdm numpy
```

## Usage

1.  **Data Configuration**:
    *   The scripts currently use hardcoded paths (e.g., `/media/volume/GLP/RDRP_6263_AUD/`).
    *   **Action**: Update the `data_path` and `notes_dir` variables in `0drugs.py`, `0ICD.py`, and `0keywords.py` to point to your local OMOP CDM formatted data.

2.  **Run Extraction Scripts**:
    Run the scripts in the following order (or in parallel) to generate the base cohorts:
    ```bash
    python 0drugs.py
    python 0ICD.py
    python 0keywords.py
    ```

3.  **Run Analysis**:
    Open `INPC.ipynb` in Jupyter Notebook to perform the overlap analysis and view the final phenotype statistics.

## Method Details

### ICD Codes
The project utilizes a comprehensive list of ICD-9 (e.g., 303.x, 305.x) and ICD-10 (e.g., F10.x) codes to capture various stages and manifestations of alcohol use disorder.

### NLP Logic
The NLP module (`0keywords.py`) implements a precise keyword matching strategy:
*   **Inclusion**: Regex matching for terms like "alcoholism", "sober", "drinking problem", etc.
*   **Exclusion**: Context-aware filtering to reduce false positives from negated phrases or references to family members.

## Results

All intermediate and final results are saved in the `results/` directory.

