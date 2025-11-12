Groundwater Data Pipeline
## Overview
This project develops an automated Extract-Load-Transform (ELT) that processes groundwater level measurements from over 24,000 monitoring wells across 23 Indian states, transforming fragmented state-wise data into a unified national dataset suitable for machine learning and agricultural forecasting.

Key Features:
Comprehensive Coverage: Processes data from 24,143 monitoring stations across 23 states
Historical Depth: Consolidates 30 years of observations (1994-2024)
Quality Assurance: Automated validation filters out ~95% of unusable files
Scalable Architecture: Modular design enables incremental monthly updates
Time-Series Optimized: Wide-format transformation for ML applications
(ELT)

Raw Data (State-wise CSVs)
    ↓
File Validation Layer
    ↓
Individual File Processing
    ↓
Intra-State Consolidation (Vertical Stack)
    ↓
Inter-State Merging (Horizontal)
    ↓
MinIO Data Lake Storage


## Data Transformation Workflow

File Validation: Checks for empty files, missing columns, and placeholder data
Long-to-Wide Pivot: Transforms station-wise observations into time-series format
Hierarchical Indexing: Creates unique identifiers (State_District_Station)
Vertical Consolidation: Combines daily files within each state
Horizontal Merging: Outer join across all states preserving complete temporal coverage

## Installation

Prerequisites
Python 3.8+
pandas >= 1.3.0

## Setup
git clone https://github.com/Quantamann/ISI-Ground-Water.git
cd ISI-Ground-Water
# Install dependencies
pip install pandas

##References

Central Ground Water Board. (2024). National Aquifer Mapping and Management Programme. http://cgwb.gov.in
India Water Resources Information System. (2024). India WRIS WebGIS Portal. https://indiawris.gov.in
