"""
Configuration file for COVID-19 Analytics Project
Contains all settings, paths, and constants used across scripts
"""

import os
from datetime import datetime

# =============================================================================
# PROJECT CONFIGURATION
# =============================================================================

PROJECT_NAME = "COVID-19 Analytics Dashboard"
PROJECT_VERSION = "1.0.0"
AUTHOR = "Your Name"

# =============================================================================
# PATHS CONFIGURATION
# =============================================================================

# Base paths (relative to scripts folder)
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_PATH, "data")
RAW_DATA_PATH = os.path.join(DATA_PATH, "raw")
PROCESSED_DATA_PATH = os.path.join(DATA_PATH, "processed")
EXTERNAL_DATA_PATH = os.path.join(DATA_PATH, "external")
NOTEBOOKS_PATH = os.path.join(BASE_PATH, "notebooks")
POWERBI_PATH = os.path.join(BASE_PATH, "powerbi")

# Ensure directories exist
for path in [DATA_PATH, RAW_DATA_PATH, PROCESSED_DATA_PATH, EXTERNAL_DATA_PATH, NOTEBOOKS_PATH, POWERBI_PATH]:
    os.makedirs(path, exist_ok=True)

# =============================================================================
# DATE CONFIGURATION
# =============================================================================

# COVID-19 started being tracked from this date
COVID_START_DATE = "2020-01-22"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")

# =============================================================================
# DATA SOURCE URLS
# =============================================================================

# Johns Hopkins University URLs
JHU_BASE_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"

JHU_URLS = {
    'confirmed_global': JHU_BASE_URL + 'time_series_covid19_confirmed_global.csv',
    'deaths_global': JHU_BASE_URL + 'time_series_covid19_deaths_global.csv',
    'confirmed_us': JHU_BASE_URL + 'time_series_covid19_confirmed_US.csv',
    'deaths_us': JHU_BASE_URL + 'time_series_covid19_deaths_US.csv'
}

# Our World in Data URLs
OWID_BASE_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/"

OWID_URLS = {
    'complete_dataset': OWID_BASE_URL + 'owid-covid-data.csv',
    'vaccinations': OWID_BASE_URL + 'vaccinations.csv',
    'vaccinations_by_manufacturer': OWID_BASE_URL + 'vaccinations-by-manufacturer.csv'
}

# Oxford Government Response Tracker
OXFORD_URL = "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_nat_latest.csv"

# Indian-specific data sources (Updated working URLs)
INDIA_DATA_URLS = {
    'national_timeseries': 'https://data.incovid19.org/csv/latest/case_time_series.csv',
    'state_wise_current': 'https://data.incovid19.org/csv/latest/state_wise.csv',
    'district_wise': 'https://data.incovid19.org/csv/latest/district_wise.csv',
    'rootnet_latest': 'https://api.rootnet.in/covid19-in/stats/latest',
    'rootnet_history': 'https://api.rootnet.in/covid19-in/unofficial/covid19india.org/statewise/history'
}

# Indian government official sources  
INDIA_OFFICIAL_URLS = {
    'mohfw_dashboard': 'https://covid19dashboard.mohfw.gov.in/',  # Ministry of Health & Family Welfare
    'icmr_website': 'https://www.icmr.gov.in/',  # ICMR official site
    'mygov_covid': 'https://www.mygov.in/covid-19/'  # Government COVID portal
}

# =============================================================================
# COUNTRIES AND REGIONS OF INTEREST
# =============================================================================

# Primary focus country
PRIMARY_COUNTRY = 'India'

# Major countries for comparison with India
MAJOR_COUNTRIES = [
    'India',  # Primary focus
    'China', 'United States', 'Brazil', 'Russia',
    'United Kingdom', 'Germany', 'France', 'Italy',
    'Japan', 'South Korea', 'Indonesia', 'Pakistan'
]

# Indian states for detailed analysis
INDIAN_STATES = [
    'Maharashtra', 'Karnataka', 'Kerala', 'Tamil Nadu', 'Uttar Pradesh',
    'Delhi', 'West Bengal', 'Gujarat', 'Rajasthan', 'Madhya Pradesh',
    'Haryana', 'Punjab', 'Telangana', 'Andhra Pradesh', 'Bihar'
]

# Regional groupings with focus on Asia and India's neighbors
REGIONS = {
    'South Asia': ['India', 'Pakistan', 'Bangladesh', 'Sri Lanka', 'Nepal', 'Bhutan', 'Maldives'],
    'East Asia': ['China', 'Japan', 'South Korea', 'Mongolia'],
    'Southeast Asia': ['Indonesia', 'Thailand', 'Singapore', 'Malaysia', 'Philippines', 'Vietnam'],
    'Europe': ['United Kingdom', 'France', 'Germany', 'Italy', 'Spain', 'Netherlands'],
    'North America': ['United States', 'Canada', 'Mexico'],
    'South America': ['Brazil', 'Argentina', 'Colombia', 'Peru'],
    'Oceania': ['Australia', 'New Zealand'],
    'Africa': ['South Africa', 'Egypt', 'Nigeria', 'Morocco']
}

# =============================================================================
# DATA PROCESSING CONFIGURATION
# =============================================================================

# Columns to keep for different analyses
ESSENTIAL_COLUMNS = {
    'time_series': ['date', 'location', 'total_cases', 'new_cases', 'total_deaths', 'new_deaths'],
    'vaccination': ['date', 'location', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated'],
    'economic': ['date', 'location', 'stringency_index', 'gdp_per_capita', 'human_development_index'],
    'testing': ['date', 'location', 'total_tests', 'new_tests', 'positive_rate']
}

# Data quality thresholds
QUALITY_THRESHOLDS = {
    'min_population': 100000,  # Minimum population for country inclusion
    'max_missing_data_pct': 50,  # Maximum percentage of missing data allowed
    'min_days_of_data': 30  # Minimum days of data required
}

# =============================================================================
# VISUALIZATION CONFIGURATION
# =============================================================================

# Color schemes for different chart types
COLOR_SCHEMES = {
    'cases': '#FF6B6B',      # Red for cases
    'deaths': '#4ECDC4',     # Teal for deaths
    'vaccinations': '#45B7D1', # Blue for vaccinations
    'recovery': '#96CEB4',   # Green for recovery
    'testing': '#FECA57'     # Yellow for testing
}

# Chart styling
CHART_STYLE = {
    'figure_size': (12, 8),
    'dpi': 300,
    'font_size': 12,
    'title_size': 16
}

# =============================================================================
# POWER BI CONFIGURATION
# =============================================================================

# Power BI connection settings
POWERBI_CONFIG = {
    'refresh_schedule': 'daily',
    'data_source_type': 'csv',  # Can be changed to 'database' later
    'max_rows_per_table': 1000000
}

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': os.path.join(BASE_PATH, 'covid_analytics.log')
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_file_path(category, filename):
    """Get full file path based on category"""
    paths = {
        'raw': RAW_DATA_PATH,
        'processed': PROCESSED_DATA_PATH,
        'external': EXTERNAL_DATA_PATH
    }
    return os.path.join(paths.get(category, RAW_DATA_PATH), filename)

def print_config_summary():
    """Print configuration summary"""
    print("=" * 60)
    print(f"🇮🇳 {PROJECT_NAME} v{PROJECT_VERSION} - India Focus")
    print("=" * 60)
    print(f"📁 Base Path: {BASE_PATH}")
    print(f"📅 Analysis Period: {COVID_START_DATE} to {CURRENT_DATE}")
    print(f"🎯 Primary Focus: {PRIMARY_COUNTRY}")
    print(f"🌍 Comparison Countries: {len(MAJOR_COUNTRIES)}")
    print(f"🏛️ Indian States Tracked: {len(INDIAN_STATES)}")
    print(f"🗺️ Regional Analysis: {len(REGIONS)} regions")
    print("=" * 60)

if __name__ == "__main__":
    print_config_summary()