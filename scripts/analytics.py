import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PowerBIDataPreparation:
    """
    Specialized class for preparing COVID-19 data for Power BI dashboards
    Creates optimized fact and dimension tables following star schema design
    """
    
    def __init__(self, data_dir: str = 'data'):
        self.data_dir = data_dir
        self.output_dir = f"{data_dir}/powerbi"
        os.makedirs(self.output_dir, exist_ok=True)
        
    def load_processed_data(self) -> pd.DataFrame:
        """Load the processed COVID-19 dataset"""
        file_path = f"{self.data_dir}/exports/processed_main_data_latest.csv"
        logger.info(f"Loading processed data from {file_path}")
        
        df = pd.read_csv(file_path, parse_dates=['date'])
        logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")
        return df
    
    def create_date_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create comprehensive date dimension table for Power BI"""
        logger.info("Creating date dimension table...")
        
        if 'date' not in df.columns:
            raise ValueError("Date column not found in dataset")
        
        # Create date range from data plus buffer
        start_date = df['date'].min() - timedelta(days=30)
        end_date = df['date'].max() + timedelta(days=365)  # Future dates for forecasting
        
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        date_dim = pd.DataFrame({
            'DateKey': date_range.strftime('%Y%m%d').astype(int),
            'Date': date_range,
            'Year': date_range.year,
            'Month': date_range.month,
            'Day': date_range.day,
            'Quarter': date_range.quarter,
            'WeekOfYear': date_range.isocalendar().week,
            'DayOfWeek': date_range.dayofweek + 1,  # 1-7 instead of 0-6
            'DayName': date_range.day_name(),
            'MonthName': date_range.month_name(),
            'MonthYear': date_range.to_period('M').astype(str),
            'QuarterYear': date_range.to_period('Q').astype(str),
            'IsWeekend': date_range.dayofweek.isin([5, 6]),
            'IsMonthStart': date_range.is_month_start,
            'IsMonthEnd': date_range.is_month_end,
            'IsQuarterStart': date_range.is_quarter_start,
            'IsQuarterEnd': date_range.is_quarter_end,
            'IsYearStart': date_range.is_year_start,
            'IsYearEnd': date_range.is_year_end
        })
        
        # Add pandemic-specific periods
        date_dim['PandemicPhase'] = pd.cut(
            date_dim['Date'],
            bins=[
                pd.Timestamp('2020-01-01'),
                pd.Timestamp('2020-06-01'),
                pd.Timestamp('2020-12-01'),
                pd.Timestamp('2021-06-01'),
                pd.Timestamp('2021-12-01'),
                pd.Timestamp('2022-06-01'),
                pd.Timestamp('2030-12-31')
            ],
            labels=[
                'Initial Outbreak',
                'First Wave',
                'Second Wave & Early Vaccination',
                'Delta Variant',
                'Omicron Surge',
                'Endemic Phase'
            ],
            include_lowest=True
        )
        
        # Add relative date calculations
        pandemic_start = pd.Timestamp('2020-03-11')  # WHO declared pandemic
        date_dim['DaysSincePandemicStart'] = (date_dim['Date'] - pandemic_start).dt.days
        date_dim['WeeksSincePandemicStart'] = date_dim['DaysSincePandemicStart'] // 7
        date_dim['MonthsSincePandemicStart'] = ((date_dim['Date'] - pandemic_start).dt.days / 30.44).astype(int)
        
        logger.info(f"Created date dimension with {len(date_dim)} records")
        return date_dim
    
    def create_geography_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create geography dimension table with hierarchical structure"""
        logger.info("Creating geography dimension table...")
        
        # Base geographical columns
        geo_columns = [
            'location', 'iso_code', 'continent'
        ]
        
        # Demographic and economic indicators
        demographic_columns = [
            'population', 'population_density', 'median_age', 'aged_65_older', 'aged_70_older'
        ]
        
        # Economic indicators
        economic_columns = [
            'gdp_per_capita', 'extreme_poverty', 'human_development_index'
        ]
        
        # Health infrastructure
        health_columns = [
            'cardiovasc_death_rate', 'diabetes_prevalence', 'female_smokers', 'male_smokers',
            'handwashing_facilities', 'hospital_beds_per_thousand', 'life_expectancy'
        ]
        
        all_columns = geo_columns + demographic_columns + economic_columns + health_columns
        available_columns = [col for col in all_columns if col in df.columns]
        
        if not available_columns:
            raise ValueError("No geography columns found in dataset")
        
        # Get unique locations with their attributes
        geo_dim = (
            df[available_columns]
            .drop_duplicates(subset=['location'])
            .reset_index(drop=True)
        )
        
        # Add geographical classifications
        geo_dim['LocationKey'] = range(1, len(geo_dim) + 1)
        
        # Add income group classification based on GDP per capita
        if 'gdp_per_capita' in geo_dim.columns:
            geo_dim['IncomeGroup'] = pd.cut(
                geo_dim['gdp_per_capita'],
                bins=[0, 1045, 4095, 12695, float('inf')],
                labels=['Low Income', 'Lower Middle Income', 'Upper Middle Income', 'High Income'],
                include_lowest=True
            )
        
        # Add population size categories
        if 'population' in geo_dim.columns:
            geo_dim['PopulationCategory'] = pd.cut(
                geo_dim['population'],
                bins=[0, 1e6, 10e6, 50e6, 100e6, float('inf')],
                labels=['<1M', '1M-10M', '10M-50M', '50M-100M', '>100M'],
                include_lowest=True
            )
        
        # Add development level based on HDI
        if 'human_development_index' in geo_dim.columns:
            geo_dim['DevelopmentLevel'] = pd.cut(
                geo_dim['human_development_index'],
                bins=[0, 0.550, 0.700, 0.800, 1.0],
                labels=['Low', 'Medium', 'High', 'Very High'],
                include_lowest=True
            )
        
        # Regional groupings
        region_mapping = {
            'Asia': ['Eastern Asia', 'South-Eastern Asia', 'Southern Asia', 'Western Asia', 'Central Asia'],
            'Europe': ['Western Europe', 'Eastern Europe', 'Northern Europe', 'Southern Europe'],
            'Africa': ['Eastern Africa', 'Western Africa', 'Northern Africa', 'Southern Africa', 'Middle Africa'],
            'North America': ['Northern America', 'Central America', 'Caribbean'],
            'South America': ['South America'],
            'Oceania': ['Australia and New Zealand', 'Melanesia', 'Micronesia', 'Polynesia']
        }
        
        # Add WHO regions (simplified)
        who_regions = {
            'AFRO': ['Africa'],
            'AMRO': ['North America', 'South America'],
            'SEARO': ['Asia'],  # Simplified - mainly South/Southeast Asia
            'EURO': ['Europe'],
            'EMRO': ['Asia'],   # Simplified - mainly Western Asia
            'WPRO': ['Asia', 'Oceania']  # Simplified - mainly Eastern Asia and Pacific
        }
        
        logger.info(f"Created geography dimension with {len(geo_dim)} locations")
        return geo_dim
    
    def create_main_fact_table(self, df: pd.DataFrame, geo_dim: pd.DataFrame) -> pd.DataFrame:
        """Create the main fact table with foreign keys and measures"""
        logger.info("Creating main fact table...")
        
        fact_table = df.copy()
        
        # Add foreign keys
        fact_table['DateKey'] = fact_table['date'].dt.strftime('%Y%m%d').astype(int)
        
        # Map to LocationKey from geography dimension
        location_map = geo_dim.set_index('location')['LocationKey'].to_dict()
        fact_table['LocationKey'] = fact_table['location'].map(location_map)
        
        # Select measure columns
        measure_columns = [
            'DateKey', 'LocationKey', 'location', 'date',
            # Core epidemiological measures
            'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
            'new_cases_smoothed', 'new_deaths_smoothed',
            
            # Per capita measures
            'total_cases_per_million', 'new_cases_per_million',
            'total_deaths_per_million', 'new_deaths_per_million',
            
            # Testing measures
            'total_tests', 'new_tests', 'positive_rate', 'tests_per_case',
            
            # Vaccination measures
            'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
            'total_boosters', 'new_vaccinations',
            'total_vaccinations_per_hundred', 'people_vaccinated_per_hundred',
            'people_fully_vaccinated_per_hundred',
            
            # Healthcare measures
            'icu_patients', 'hosp_patients', 'weekly_icu_admissions', 'weekly_hosp_admissions',
            
            # Policy measures
            'stringency_index', 'reproduction_rate',
            
            # Derived measures
            'case_fatality_rate', 'vaccination_period'
        ]
        
        # Add rolling averages if they exist
        rolling_columns = [col for col in fact_table.columns if '_rolling_' in col or '_sum_' in col]
        measure_columns.extend(rolling_columns)
        
        # Filter to available columns
        available_measures = [col for col in measure_columns if col in fact_table.columns]
        fact_table = fact_table[available_measures]
        
        # Ensure proper data types
        numeric_columns = fact_table.select_dtypes(include=[np.number]).columns
        fact_table[numeric_columns] = fact_table[numeric_columns].fillna(0)
        
        # Add calculated measures for Power BI
        fact_table['HasNewCases'] = (fact_table['new_cases'] > 0).astype(int)
        fact_table['HasNewDeaths'] = (fact_table['new_deaths'] > 0).astype(int)
        
        if 'people_fully_vaccinated_per_hundred' in fact_table.columns:
            fact_table['VaccinationStatus'] = pd.cut(
                fact_table['people_fully_vaccinated_per_hundred'],
                bins=[0, 25, 50, 75, 100],
                labels=['Low (<25%)', 'Medium (25-50%)', 'High (50-75%)', 'Very High (>75%)'],
                include_lowest=True
            )
        
        logger.info(f"Created fact table with {len(fact_table)} records and {len(fact_table.columns)} columns")
        return fact_table
    
    def create_vaccination_fact_table(self, df: pd.DataFrame, geo_dim: pd.DataFrame) -> pd.DataFrame:
        """Create specialized fact table for vaccination analysis"""
        logger.info("Creating vaccination fact table...")
        
        vax_columns = [
            'location', 'date', 'total_vaccinations', 'people_vaccinated',
            'people_fully_vaccinated', 'total_boosters', 'new_vaccinations',
            'total_vaccinations_per_hundred', 'people_vaccinated_per_hundred',
            'people_fully_vaccinated_per_hundred', 'total_boosters_per_hundred'
        ]
        
        available_vax_columns = [col for col in vax_columns if col in df.columns]
        
        if len(available_vax_columns) < 3:  # At least location, date, and one measure
            logger.warning("Insufficient vaccination data for vaccination fact table")
            return pd.DataFrame()
        
        vax_fact = df[available_vax_columns].copy()
        vax_fact = vax_fact.dropna(subset=available_vax_columns[2:], how='all')  # Remove rows with no vaccination data
        
        # Add foreign keys
        vax_fact['DateKey'] = pd.to_datetime(vax_fact['date']).dt.strftime('%Y%m%d').astype(int)
        location_map = geo_dim.set_index('location')['LocationKey'].to_dict()
        vax_fact['LocationKey'] = vax_fact['location'].map(location_map)
        
        # Calculate additional vaccination metrics
        if all(col in vax_fact.columns for col in ['people_vaccinated', 'people_fully_vaccinated']):
            vax_fact['people_partially_vaccinated'] = (
                vax_fact['people_vaccinated'] - vax_fact['people_fully_vaccinated']
            ).clip(lower=0)
        
        # Add vaccination milestones
        if 'people_fully_vaccinated_per_hundred' in vax_fact.columns:
            vax_fact['reached_50_percent'] = (vax_fact['people_fully_vaccinated_per_hundred'] >= 50).astype(int)
            vax_fact['reached_70_percent'] = (vax_fact['people_fully_vaccinated_per_hundred'] >= 70).astype(int)
        
        logger.info(f"Created vaccination fact table with {len(vax_fact)} records")
        return vax_fact
    
    def create_testing_fact_table(self, df: pd.DataFrame, geo_dim: pd.DataFrame) -> pd.DataFrame:
        """Create specialized fact table for testing analysis"""
        logger.info("Creating testing fact table...")
        
        test_columns = [
            'location', 'date', 'total_tests', 'new_tests',
            'total_tests_per_thousand', 'new_tests_per_thousand',
            'positive_rate', 'tests_per_case', 'tests_units'
        ]
        
        available_test_columns = [col for col in test_columns if col in df.columns]
        
        if len(available_test_columns) < 3:
            logger.warning("Insufficient testing data for testing fact table")
            return pd.DataFrame()
        
        test_fact = df[available_test_columns].copy()
        test_fact = test_fact.dropna(subset=available_test_columns[2:5], how='all')  # Keep if has any test metrics
        
        # Add foreign keys
        test_fact['DateKey'] = pd.to_datetime(test_fact['date']).dt.strftime('%Y%m%d').astype(int)
        location_map = geo_dim.set_index('location')['LocationKey'].to_dict()
        test_fact['LocationKey'] = test_fact['location'].map(location_map)
        
        # Add testing intensity categories
        if 'total_tests_per_thousand' in test_fact.columns:
            test_fact['TestingIntensity'] = pd.cut(
                test_fact['total_tests_per_thousand'],
                bins=[0, 100, 500, 1000, float('inf')],
                labels=['Low (<100)', 'Medium (100-500)', 'High (500-1000)', 'Very High (>1000)'],
                include_lowest=True
            )
        
        # Add positivity rate categories
        if 'positive_rate' in test_fact.columns:
            test_fact['PositivityCategory'] = pd.cut(
                test_fact['positive_rate'],
                bins=[0, 5, 10, 20, float('inf')],
                labels=['Low (<5%)', 'Medium (5-10%)', 'High (10-20%)', 'Very High (>20%)'],
                include_lowest=True
            )
        
        logger.info(f"Created testing fact table with {len(test_fact)} records")
        return test_fact
    
    def create_policy_fact_table(self, df: pd.DataFrame, geo_dim: pd.DataFrame) -> pd.DataFrame:
        """Create fact table for policy measures and interventions"""
        logger.info("Creating policy fact table...")
        
        policy_columns = [
            'location', 'date', 'stringency_index', 'reproduction_rate'
        ]
        
        available_policy_columns = [col for col in policy_columns if col in df.columns]
        
        if len(available_policy_columns) < 3:
            logger.warning("Insufficient policy data for policy fact table")
            return pd.DataFrame()
        
        policy_fact = df[available_policy_columns].copy()
        policy_fact = policy_fact.dropna(subset=['stringency_index'], how='all')
        
        # Add foreign keys
        policy_fact['DateKey'] = pd.to_datetime(policy_fact['date']).dt.strftime('%Y%m%d').astype(int)
        location_map = geo_dim.set_index('location')['LocationKey'].to_dict()
        policy_fact['LocationKey'] = policy_fact['location'].map(location_map)
        
        # Add policy stringency categories
        if 'stringency_index' in policy_fact.columns:
            policy_fact['StringencyLevel'] = pd.cut(
                policy_fact['stringency_index'],
                bins=[0, 25, 50, 75, 100],
                labels=['Low (0-25)', 'Medium (25-50)', 'High (50-75)', 'Very High (75-100)'],
                include_lowest=True
            )
        
        # Add reproduction rate categories
        if 'reproduction_rate' in policy_fact.columns:
            policy_fact['TransmissionLevel'] = pd.cut(
                policy_fact['reproduction_rate'],
                bins=[0, 0.8, 1.0, 1.5, float('inf')],
                labels=['Declining (<0.8)', 'Controlled (0.8-1.0)', 'Growing (1.0-1.5)', 'Rapid Growth (>1.5)'],
                include_lowest=True
            )
        
        logger.info(f"Created policy fact table with {len(policy_fact)} records")
        return policy_fact
    
    def create_kpi_summary_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create a summary table with key performance indicators"""
        logger.info("Creating KPI summary table...")
        
        # Get latest data for each location
        latest_data = df.sort_values('date').groupby('location').last().reset_index()
        
        kpi_table = latest_data[['location', 'iso_code', 'continent', 'population']].copy()
        
        # Add core KPIs
        kpi_columns = {
            'total_cases': 'TotalCases',
            'total_deaths': 'TotalDeaths',
            'total_cases_per_million': 'CasesPerMillion',
            'total_deaths_per_million': 'DeathsPerMillion',
            'case_fatality_rate': 'CaseFatalityRate',
            'people_fully_vaccinated_per_hundred': 'FullyVaccinatedPercent',
            'total_tests_per_thousand': 'TestsPerThousand',
            'stringency_index': 'CurrentStringencyIndex',
            'reproduction_rate': 'CurrentReproductionRate'
        }
        
        for original_col, new_col in kpi_columns.items():
            if original_col in latest_data.columns:
                kpi_table[new_col] = latest_data[original_col]
        
        # Calculate additional KPIs
        if all(col in latest_data.columns for col in ['total_deaths', 'total_cases']):
            kpi_table['CaseFatalityRate'] = (
                latest_data['total_deaths'] / latest_data['total_cases'] * 100
            ).fillna(0)
        
        # Add ranking columns
        if 'TotalCases' in kpi_table.columns:
            kpi_table['CasesRank'] = kpi_table['TotalCases'].rank(method='dense', ascending=False)
        
        if 'CasesPerMillion' in kpi_table.columns:
            kpi_table['CasesPerMillionRank'] = kpi_table['CasesPerMillion'].rank(method='dense', ascending=False)
        
        # Add status indicators
        if 'FullyVaccinatedPercent' in kpi_table.columns:
            kpi_table['VaccinationStatus'] = pd.cut(
                kpi_table['FullyVaccinatedPercent'],
                bins=[0, 40, 70, 90, 100],
                labels=['Low', 'Medium', 'High', 'Very High'],
                include_lowest=True
            )
        
        logger.info(f"Created KPI summary table with {len(kpi_table)} locations")
        return kpi_table
    
    def export_for_powerbi(self, tables: Dict[str, pd.DataFrame]):
        """Export all tables in Power BI friendly formats"""
        logger.info("Exporting tables for Power BI...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for table_name, df in tables.items():
            if df.empty:
                logger.warning(f"Skipping empty table: {table_name}")
                continue
            
            # Excel format for Power BI import
            excel_file = f"{self.output_dir}/{table_name}.xlsx"
            df.to_excel(excel_file, index=False)
            logger.info(f"Exported {table_name} to Excel: {len(df)} rows")
            
            # CSV format as backup
            csv_file = f"{self.output_dir}/{table_name}.csv"
            df.to_csv(csv_file, index=False)
            
            # Create timestamped backup
            backup_file = f"{self.output_dir}/backup/{table_name}_{timestamp}.xlsx"
            os.makedirs(f"{self.output_dir}/backup", exist_ok=True)
            df.to_excel(backup_file, index=False)
    
    def create_powerbi_measures_file(self):
        """Create a text file with DAX measure definitions for Power BI"""
        logger.info("Creating DAX measures file...")
        
        dax_measures = """
-- COVID-19 Dashboard DAX Measures
-- Copy and paste these measures into Power BI

-- Core KPIs
Total Cases = SUM(FactCovid[total_cases])
Total Deaths = SUM(FactCovid[total_deaths])
New Cases = SUM(FactCovid[new_cases])
New Deaths = SUM(FactCovid[new_deaths])

-- Rolling Averages
New Cases 7-Day Avg = 
AVERAGEX(
    DATESINPERIOD(DimDate[Date], MAX(DimDate[Date]), -7, DAY),
    CALCULATE(SUM(FactCovid[new_cases]))
)

New Deaths 7-Day Avg = 
AVERAGEX(
    DATESINPERIOD(DimDate[Date], MAX(DimDate[Date]), -7, DAY),
    CALCULATE(SUM(FactCovid[new_deaths]))
)

-- Vaccination Metrics
Vaccination Rate = 
DIVIDE(
    SUM(FactVaccination[people_fully_vaccinated]),
    SUM(DimGeography[population])
) * 100

-- Testing Metrics
Positivity Rate = 
DIVIDE(
    SUM(FactTesting[new_cases]),
    SUM(FactTesting[new_tests])
) * 100

-- Growth Rates
Case Growth Rate = 
VAR CurrentCases = SUM(FactCovid[new_cases])
VAR PreviousCases = 
    CALCULATE(
        SUM(FactCovid[new_cases]),
        DATEADD(DimDate[Date], -7, DAY)
    )
RETURN
    DIVIDE(CurrentCases - PreviousCases, PreviousCases) * 100

-- Comparative Measures
Cases vs Previous Period = 
VAR CurrentPeriod = SUM(FactCovid[total_cases])
VAR PreviousPeriod = 
    CALCULATE(
        SUM(FactCovid[total_cases]),
        DATEADD(DimDate[Date], -7, DAY)
    )
RETURN
    CurrentPeriod - PreviousPeriod

-- Status Indicators
Trend Indicator = 
VAR CurrentWeek = [New Cases 7-Day Avg]
VAR PreviousWeek = 
    CALCULATE(
        [New Cases 7-Day Avg],
        DATEADD(DimDate[Date], -7, DAY)
    )
RETURN
    IF(
        CurrentWeek > PreviousWeek * 1.1, "🔴 Increasing",
        IF(CurrentWeek < PreviousWeek * 0.9, "🟢 Decreasing", "🟡 Stable")
    )

-- Ranking Measures
Country Cases Rank = 
RANKX(
    ALL(DimGeography[location]),
    [Total Cases],
    ,
    DESC
)

-- Date Intelligence
Is Latest Date = 
IF(
    MAX(DimDate[Date]) = CALCULATE(MAX(DimDate[Date]), ALL(DimDate)),
    TRUE(),
    FALSE()
)

Days Since Pandemic Start = 
DATEDIFF(
    DATE(2020,3,11),
    MAX(DimDate[Date]),
    DAY
)

-- Conditional Formatting Helpers
Cases Color = 
VAR CasesPerMillion = DIVIDE([Total Cases], SUM(DimGeography[population])) * 1000000
RETURN
    SWITCH(
        TRUE(),
        CasesPerMillion < 10000, "#90EE90",
        CasesPerMillion < 50000, "#FFD700",
        CasesPerMillion < 100000, "#FFA500",
        "#FF6347"
    )

Vaccination Color = 
VAR VaxRate = [Vaccination Rate]
RETURN
    SWITCH(
        TRUE(),
        VaxRate < 25, "#FF6347",
        VaxRate < 50, "#FFA500",
        VaxRate < 75, "#FFD700",
        "#90EE90"
    )
"""
        
        measures_file = f"{self.output_dir}/DAX_Measures.txt"
        with open(measures_file, 'w') as f:
            f.write(dax_measures)
        
        logger.info(f"DAX measures saved to {measures_file}")
    
    def create_powerbi_setup_guide(self):
        """Create a setup guide for Power BI dashboard creation"""
        logger.info("Creating Power BI setup guide...")
        
        setup_guide = """
# Power BI COVID-19 Dashboard Setup Guide

## Step 1: Import Data Tables
1. Open Power BI Desktop
2. Go to Home > Get Data > Excel
3. Import the following tables from the powerbi folder:
   - DimDate.xlsx (Date Dimension)
   - DimGeography.xlsx (Geography Dimension)
   - FactCovid.xlsx (Main COVID Facts)
   - FactVaccination.xlsx (Vaccination Facts)
   - FactTesting.xlsx (Testing Facts)
   - FactPolicy.xlsx (Policy Facts)
   - KPISummary.xlsx (KPI Summary)

## Step 2: Create Relationships
Set up the following relationships in Model View:
- DimDate[DateKey] → FactCovid[DateKey] (One-to-Many)
- DimGeography[LocationKey] → FactCovid[LocationKey] (One-to-Many)
- DimDate[DateKey] → FactVaccination[DateKey] (One-to-Many)
- DimGeography[LocationKey] → FactVaccination[LocationKey] (One-to-Many)
- Similar relationships for FactTesting and FactPolicy

## Step 3: Add DAX Measures
1. Copy measures from DAX_Measures.txt
2. Create a new table called "Measures"
3. Paste each measure using the "New Measure" option

## Step 4: Dashboard Pages to Create

### Page 1: Global Overview
- KPI cards: Total Cases, Deaths, Countries Affected, Vaccination Rate
- World map: Cases/Deaths by country
- Line chart: Global daily trends
- Bar chart: Top 10 affected countries

### Page 2: Regional Analysis
- Slicer: Continent selector
- Table: Country details with metrics
- Area chart: Regional case trends
- Scatter plot: Cases vs Deaths per million

### Page 3: Vaccination Tracker
- KPI cards: Global vaccination metrics
- Progress bars: Vaccination targets by country
- Timeline: Vaccination rollout progress
- Heatmap: Vaccination rates by region

### Page 4: Testing Analysis
- Line chart: Testing trends
- Scatter plot: Testing vs Positivity rates
- Bar chart: Tests per capita by country
- KPI: Global positivity rate

### Page 5: Policy Impact
- Line chart: Stringency index over time
- Correlation chart: Policy measures vs case trends
- Table: Current policy status by country

## Step 5: Formatting Tips
- Use consistent color schemes (Red for cases/deaths, Blue for tests, Green for vaccinations)
- Add tooltips with additional context
- Enable drill-down functionality
- Use conditional formatting for KPI cards
- Add filters for date ranges and countries

## Step 6: Publishing and Sharing
1. Save the .pbix file
2. Publish to Power BI Service
3. Set up automatic data refresh
4. Share with stakeholders
5. Create mobile-optimized views

## Data Refresh Schedule
- Set up daily refresh for latest data
- Monitor for data quality issues
- Update DAX measures as needed
- Archive old reports monthly
"""
        
        guide_file = f"{self.output_dir}/PowerBI_Setup_Guide.md"
        with open(guide_file, 'w') as f:
            f.write(setup_guide)
        
        logger.info(f"Setup guide saved to {guide_file}")
    
    def prepare_all_powerbi_tables(self) -> Dict[str, pd.DataFrame]:
        """Execute the complete Power BI data preparation pipeline"""
        logger.info("=== Starting Power BI Data Preparation ===")
        
        # Load processed data
        df = self.load_processed_data()
        
        # Create all dimension and fact tables
        tables = {}
        
        # Dimension tables
        tables['DimDate'] = self.create_date_dimension(df)
        tables['DimGeography'] = self.create_geography_dimension(df)
        
        # Fact tables
        tables['FactCovid'] = self.create_main_fact_table(df, tables['DimGeography'])
        tables['FactVaccination'] = self.create_vaccination_fact_table(df, tables['DimGeography'])
        tables['FactTesting'] = self.create_testing_fact_table(df, tables['DimGeography'])
        tables['FactPolicy'] = self.create_policy_fact_table(df, tables['DimGeography'])
        
        # Summary tables
        tables['KPISummary'] = self.create_kpi_summary_table(df)
        
        # Export everything
        self.export_for_powerbi(tables)
        
        # Create supporting files
        self.create_powerbi_measures_file()
        self.create_powerbi_setup_guide()
        
        logger.info("=== Power BI Data Preparation Complete ===")
        logger.info(f"Created {len(tables)} tables for Power BI")
        
        # Print summary
        for name, table in tables.items():
            if not table.empty:
                logger.info(f"{name}: {len(table)} rows, {len(table.columns)} columns")
        
        return tables

def main():
    """Main execution function"""
    prep = PowerBIDataPreparation()
    tables = prep.prepare_all_powerbi_tables()
    
    print("\n=== Power BI Data Preparation Summary ===")
    print(f"Created {len(tables)} tables:")
    
    for name, df in tables.items():
        if not df.empty:
            print(f"  ✓ {name}: {df.shape[0]:,} rows × {df.shape[1]} columns")
        else:
            print(f"  ⚠ {name}: Empty table (insufficient data)")
    
    print(f"\nFiles exported to: data/powerbi/")
    print("Ready for Power BI import!")
    
    return tables

if __name__ == "__main__":
    powerbi_tables = main()