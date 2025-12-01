"""Configuration settings for the data analysis project"""

import os

# Base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# File paths
ACCOUNTS_FILE = os.path.join(DATA_DIR, 'accounts_anonymized.json')
SUPPORT_CASES_FILE = os.path.join(DATA_DIR, 'support_cases_anonymized.json')

# Output settings
OUTPUT_DIR = os.path.join(BASE_DIR, 'outputs')
VISUALIZATIONS_DIR = os.path.join(OUTPUT_DIR, 'visualizations')

# Create directories if they don't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(VISUALIZATIONS_DIR, exist_ok=True)

# Visualization settings
FIGURE_SIZE = (12, 6)
DPI = 300
COLOR_PALETTE = 'viridis'

# Analysis parameters
TOP_N_ACCOUNTS = 15
TOP_N_COUNTRIES = 15
TOP_N_INDUSTRIES = 12