import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CovidDataProcessor:
    """
    Comprehensive data processing pipeline for COVID-19 data analysis
    """
    
    def __init__(self, data_dir: str = 'data'):
        self.data_dir = data_dir
        self.processed_data = {}
        
    def load_raw_data(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """Load raw COVID-19 data"""
        if file_path is None:
            file_path = f"{self.data_dir}/raw/owid_covid_data_latest.csv"
        
        logger.info(f"Loading data from {file_path}")
        df = pd.read_csv(file_path, low_memory=False)
        logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns")
        return df
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the dataset"""
        logger.info("Starting data cleaning...")
        df_clean = df.copy()
        
        # Convert date column
        if 'date' in df_clean.columns:
            df_clean['date'] = pd.to_datetime(df_clean['date'])
            logger.info("Date column converted to datetime")
        
        # Clean location names (remove extra spaces, standardize)
        if 'location' in df_clean.columns:
            df_clean['location'] = df_clean['location'].str.strip()
        
        # Handle inf values
        numeric_columns = df_clean.select_dtypes(include=[np.number]).columns
        df_clean[numeric_columns] = df_clean[numeric_columns].replace([np.inf, -np.inf], np.nan)
        
        # Remove duplicate rows
        initial_rows = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        dropped_rows = initial_rows - len(df_clean)
        if dropped_rows > 0:
            logger.info(f"Removed {dropped_rows} duplicate rows")
        
        # Sort by location and date
        if all(col in df_clean.columns for col in ['location', 'date']):
            df_clean = df_clean.sort_values(['location', 'date'])
        
        logger.info("Data cleaning completed")
        return df_clean
    
    def create_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create additional calculated metrics"""
        logger.info("Creating derived metrics...")
        df_enhanced = df.copy()
        
        # Case fatality rate
        if all(col in df.columns for col in ['total_deaths', 'total_cases']):
            df_enhanced['case_fatality_rate'] = (
                df_enhanced['total_deaths'] / df_enhanced['total_cases'] * 100
            ).fillna(0)
        
        # Daily change rates
        if 'new_cases' in df.columns and 'total_cases' in df.columns:
            df_enhanced['case_growth_rate'] = (
                df_enhanced['new_cases'] / df_enhanced['total_cases'] * 100
            ).fillna(0)
        
        # Deaths per million (if not already present)
        if all(col in df.columns for col in ['total_deaths', 'population']):
            if 'total_deaths_per_million' not in df.columns:
                df_enhanced['total_deaths_per_million'] = (
                    df_enhanced['total_deaths'] / df_enhanced['population'] * 1_000_000
                )
        
        # Vaccination effectiveness proxy
        if all(col in df.columns for col in ['people_fully_vaccinated_per_hundred', 'new_deaths_per_million']):
            df_enhanced['vaccination_period'] = np.where(
                df_enhanced['people_fully_vaccinated_per_hundred'] > 50,
                'High Vaccination',
                'Low Vaccination'
            )
        
        # Testing positivity rate (if not present)
        if all(col in df.columns for col in ['new_cases', 'new_tests']) and 'positive_rate' not in df.columns:
            df_enhanced['positive_rate'] = (
                df_enhanced['new_cases'] / df_enhanced['new_tests'] * 100
            ).clip(0, 100)
        
        # Time-based features
        if 'date' in df.columns:
            df_enhanced['year'] = df_enhanced['date'].dt.year
            df_enhanced['month'] = df_enhanced['date'].dt.month
            df_enhanced['quarter'] = df_enhanced['date'].dt.quarter
            df_enhanced['day_of_week'] = df_enhanced['date'].dt.dayofweek
            df_enhanced['week_of_year'] = df_enhanced['date'].dt.isocalendar().week
        
        # Pandemic phase classification
        df_enhanced['pandemic_phase'] = self._classify_pandemic_phase(df_enhanced)
        
        logger.info("Derived metrics created")
        return df_enhanced
    
    def _classify_pandemic_phase(self, df: pd.DataFrame) -> pd.Series:
        """Classify pandemic phases based on date"""
        if 'date' not in df.columns:
            return pd.Series(['Unknown'] * len(df))
        
        conditions = [
            df['date'] < '2020-06-01',  # Initial outbreak
            df['date'] < '2020-12-01',  # First wave
            df['date'] < '2021-06-01',  # Second wave & early vaccination
            df['date'] < '2021-12-01',  # Delta variant
            df['date'] < '2022-06-01',  # Omicron surge
            df['date'] >= '2022-06-01'  # Endemic phase
        ]
        
        choices = [
            'Initial Outbreak',
            'First Wave',
            'Second Wave',
            'Delta Variant',
            'Omicron Surge',
            'Endemic Phase'
        ]
        
        return pd.Series(np.select(conditions, choices, default='Unknown'))
    
    def calculate_rolling_metrics(self, df: pd.DataFrame, windows: List[int] = [7, 14, 30]) -> pd.DataFrame:
        """Calculate rolling averages and trends"""
        logger.info(f"Calculating rolling metrics for windows: {windows}")
        df_rolling = df.copy()
        
        # Ensure data is sorted by location and date
        if all(col in df.columns for col in ['location', 'date']):
            df_rolling = df_rolling.sort_values(['location', 'date'])
        
        # Metrics to calculate rolling averages for
        metrics = ['new_cases', 'new_deaths', 'new_tests', 'new_vaccinations']
        available_metrics = [col for col in metrics if col in df.columns]
        
        for window in windows:
            for metric in available_metrics:
                # Rolling average
                col_name = f"{metric}_rolling_{window}d"
                df_rolling[col_name] = (
                    df_rolling.groupby('location')[metric]
                    .rolling(window=window, min_periods=1)
                    .mean()
                    .reset_index(0, drop=True)
                )
                
                # Rolling sum
                sum_col_name = f"{metric}_sum_{window}d"
                df_rolling[sum_col_name] = (
                    df_rolling.groupby('location')[metric]
                    .rolling(window=window, min_periods=1)
                    .sum()
                    .reset_index(0, drop=True)
                )
        
        # Calculate trends (7-day vs 14-day comparison)
        if 'new_cases_rolling_7d' in df_rolling.columns and 'new_cases_rolling_14d' in df_rolling.columns:
            df_rolling['case_trend'] = np.where(
                df_rolling['new_cases_rolling_7d'] > df_rolling['new_cases_rolling_14d'],
                'Increasing',
                np.where(
                    df_rolling['new_cases_rolling_7d'] < df_rolling['new_cases_rolling_14d'],
                    'Decreasing',
                    'Stable'
                )
            )
        
        logger.info("Rolling metrics calculated")
        return df_rolling
    
    def create_aggregated_datasets(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create various aggregated views of the data"""
        logger.info("Creating aggregated datasets...")
        aggregated = {}
        
        # 1. Latest data by country
        if all(col in df.columns for col in ['location', 'date']):
            latest_data = (
                df.sort_values('date')
                .groupby('location')
                .last()
                .reset_index()
            )
            aggregated['latest_by_country'] = latest_data
        
        # 2. Continental aggregation
        if 'continent' in df.columns:
            continent_agg = (
                df.groupby(['continent', 'date'])
                .agg({
                    'new_cases': 'sum',
                    'new_deaths': 'sum',
                    'total_cases': 'sum',
                    'total_deaths': 'sum',
                    'population': 'sum'
                })
                .reset_index()
            )
            # Recalculate per capita metrics
            continent_agg['cases_per_million'] = (
                continent_agg['total_cases'] / continent_agg['population'] * 1_000_000
            )
            continent_agg['deaths_per_million'] = (
                continent_agg['total_deaths'] / continent_agg['population'] * 1_000_000
            )
            aggregated['continental_data'] = continent_agg
        
        # 3. Monthly aggregation
        if 'date' in df.columns:
            df_monthly = df.copy()
            df_monthly['year_month'] = df_monthly['date'].dt.to_period('M')
            monthly_agg = (
                df_monthly.groupby(['location', 'year_month'])
                .agg({
                    'new_cases': 'sum',
                    'new_deaths': 'sum',
                    'new_tests': 'sum',
                    'new_vaccinations': 'sum',
                    'total_cases': 'last',
                    'total_deaths': 'last',
                    'population': 'last'
                })
                .reset_index()
            )
            aggregated['monthly_data'] = monthly_agg
        
        # 4. Global daily totals
        if 'date' in df.columns:
            global_daily = (
                df[df['location'] != 'World']  # Exclude world totals to avoid double counting
                .groupby('date')
                .agg({
                    'new_cases': 'sum',
                    'new_deaths': 'sum',
                    'new_tests': 'sum',
                    'total_cases': 'sum',
                    'total_deaths': 'sum'
                })
                .reset_index()
            )
            aggregated['global_daily'] = global_daily
        
        # 5. Top countries by metrics
        if 'location' in df.columns:
            # Get latest data for ranking
            latest = df.groupby('location').last().reset_index()
            
            # Top 20 by total cases
            top_cases = latest.nlargest(20, 'total_cases')[
                ['location', 'total_cases', 'total_deaths', 'population']
            ]
            aggregated['top_countries_cases'] = top_cases
            
            # Top 20 by deaths per million
            if 'total_deaths_per_million' in latest.columns:
                top_deaths_per_mil = latest.nlargest(20, 'total_deaths_per_million')[
                    ['location', 'total_deaths_per_million', 'total_cases_per_million']
                ]
                aggregated['top_countries_deaths_per_million'] = top_deaths_per_mil
        
        logger.info(f"Created {len(aggregated)} aggregated datasets")
        return aggregated
    
    def create_powerbi_ready_datasets(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create datasets optimized for Power BI consumption"""
        logger.info("Creating Power BI ready datasets...")
        powerbi_data = {}
        
        # 1. Main fact table (daily data)
        fact_table = df.copy()
        
        # Select key columns for fact table
        fact_columns = [
            'location', 'date', 'iso_code', 'continent',
            'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
            'total_tests', 'new_tests', 'positive_rate',
            'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
            'hosp_patients', 'icu_patients', 'stringency_index',
            'reproduction_rate', 'case_fatality_rate'
        ]
        
        available_fact_columns = [col for col in fact_columns if col in fact_table.columns]
        fact_table = fact_table[available_fact_columns]
        
        # Add surrogate keys
        fact_table['date_key'] = fact_table['date'].dt.strftime('%Y%m%d').astype(int)
        fact_table['location_key'] = fact_table['location'].astype('category').cat.codes
        
        powerbi_data['fact_daily_covid'] = fact_table
        
        # 2. Date dimension table
        if 'date' in df.columns:
            date_range = pd.date_range(
                start=df['date'].min(),
                end=df['date'].max(),
                freq='D'
            )
            
            date_dim = pd.DataFrame({
                'date': date_range,
                'date_key': date_range.strftime('%Y%m%d').astype(int),
                'year': date_range.year,
                'month': date_range.month,
                'day': date_range.day,
                'quarter': date_range.quarter,
                'week_of_year': date_range.isocalendar().week,
                'day_of_week': date_range.dayofweek,
                'day_name': date_range.day_name(),
                'month_name': date_range.month_name(),
                'is_weekend': date_range.dayofweek.isin([5, 6])
            })
            
            powerbi_data['dim_date'] = date_dim
        
        # 3. Location dimension table
        if 'location' in df.columns:
            location_cols = [
                'location', 'iso_code', 'continent', 'population',
                'population_density', 'median_age', 'aged_65_older',
                'gdp_per_capita', 'cardiovasc_death_rate', 'diabetes_prevalence',
                'hospital_beds_per_thousand', 'life_expectancy', 'human_development_index'
            ]
            
            available_location_cols = [col for col in location_cols if col in df.columns]
            location_dim = (
                df[available_location_cols]
                .drop_duplicates(subset=['location'])
                .reset_index(drop=True)
            )
            
            location_dim['location_key'] = location_dim['location'].astype('category').cat.codes
            powerbi_data['dim_location'] = location_dim
        
        # 4. Vaccination summary table
        vax_columns = [
            'location', 'date', 'total_vaccinations', 'people_vaccinated',
            'people_fully_vaccinated', 'total_boosters', 'new_vaccinations'
        ]
        
        available_vax_columns = [col for col in vax_columns if col in df.columns]
        if len(available_vax_columns) > 2:  # At least location, date and one metric
            vax_data = df[available_vax_columns].dropna(subset=available_vax_columns[2:], how='all')
            powerbi_data['fact_vaccinations'] = vax_data
        
        # 5. Testing summary table
        test_columns = [
            'location', 'date', 'total_tests', 'new_tests',
            'positive_rate', 'tests_per_case'
        ]
        
        available_test_columns = [col for col in test_columns if col in df.columns]
        if len(available_test_columns) > 2:
            test_data = df[available_test_columns].dropna(subset=available_test_columns[2:], how='all')
            powerbi_data['fact_testing'] = test_data
        
        # 6. Policy measures table
        policy_columns = [
            'location', 'date', 'stringency_index'
        ]
        
        available_policy_columns = [col for col in policy_columns if col in df.columns]
        if len(available_policy_columns) > 2:
            policy_data = df[available_policy_columns].dropna()
            powerbi_data['fact_policy_measures'] = policy_data
        
        logger.info(f"Created {len(powerbi_data)} Power BI ready datasets")
        return powerbi_data
    
    def export_processed_data(self, datasets: Dict[str, pd.DataFrame], format_type: str = 'csv'):
        """Export processed datasets in various formats"""
        logger.info(f"Exporting datasets in {format_type} format...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for name, df in datasets.items():
            if format_type.lower() == 'csv':
                file_path = f"{self.data_dir}/exports/{name}_{timestamp}.csv"
                df.to_csv(file_path, index=False)
            elif format_type.lower() == 'excel':
                file_path = f"{self.data_dir}/exports/{name}_{timestamp}.xlsx"
                df.to_excel(file_path, index=False)
            elif format_type.lower() == 'parquet':
                file_path = f"{self.data_dir}/exports/{name}_{timestamp}.parquet"
                df.to_parquet(file_path, index=False)
            
            logger.info(f"Exported {name} to {file_path}")
            
            # Also save as latest version
            latest_path = file_path.replace(f"_{timestamp}", "_latest")
            if format_type.lower() == 'csv':
                df.to_csv(latest_path, index=False)
            elif format_type.lower() == 'excel':
                df.to_excel(latest_path, index=False)
            elif format_type.lower() == 'parquet':
                df.to_parquet(latest_path, index=False)
    
    def generate_data_quality_report(self, df: pd.DataFrame) -> Dict:
        """Generate comprehensive data quality report"""
        logger.info("Generating data quality report...")
        
        report = {
            'overview': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
                'generated_at': datetime.now().isoformat()
            },
            'missing_data': {},
            'data_types': {},
            'outliers': {},
            'duplicates': {},
            'date_coverage': {}
        }
        
        # Missing data analysis
        missing_counts = df.isnull().sum()
        missing_percentages = (missing_counts / len(df)) * 100
        
        report['missing_data'] = {
            'columns_with_missing': missing_counts[missing_counts > 0].to_dict(),
            'missing_percentages': missing_percentages[missing_percentages > 0].to_dict(),
            'completely_missing_columns': missing_counts[missing_counts == len(df)].index.tolist()
        }
        
        # Data types
        report['data_types'] = df.dtypes.astype(str).to_dict()
        
        # Duplicates
        duplicate_count = df.duplicated().sum()
        report['duplicates'] = {
            'total_duplicates': int(duplicate_count),
            'duplicate_percentage': float(duplicate_count / len(df) * 100)
        }
        
        # Date coverage analysis
        if 'date' in df.columns:
            date_stats = {
                'date_range_start': df['date'].min().isoformat(),
                'date_range_end': df['date'].max().isoformat(),
                'total_days': (df['date'].max() - df['date'].min()).days + 1,
                'unique_dates': df['date'].nunique(),
                'missing_dates': []
            }
            
            # Find missing dates in the range
            full_date_range = pd.date_range(
                start=df['date'].min(),
                end=df['date'].max(),
                freq='D'
            )
            missing_dates = set(full_date_range) - set(df['date'].unique())
            date_stats['missing_dates'] = [d.isoformat() for d in sorted(missing_dates)]
            
            report['date_coverage'] = date_stats
        
        # Outlier detection for key numeric columns
        numeric_columns = ['new_cases', 'new_deaths', 'total_cases', 'total_deaths']
        available_numeric = [col for col in numeric_columns if col in df.columns]
        
        for col in available_numeric:
            if df[col].dtype in ['int64', 'float64'] and not df[col].isnull().all():
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
                
                report['outliers'][col] = {
                    'outlier_count': len(outliers),
                    'outlier_percentage': len(outliers) / len(df) * 100,
                    'lower_bound': float(lower_bound),
                    'upper_bound': float(upper_bound)
                }
        
        return report
    
    def process_full_pipeline(self, input_file: Optional[str] = None) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """Execute the complete data processing pipeline"""
        logger.info("=== Starting Full Data Processing Pipeline ===")
        
        # Step 1: Load raw data
        df = self.load_raw_data(input_file)
        
        # Step 2: Clean data
        df_clean = self.clean_data(df)
        
        # Step 3: Create derived metrics
        df_enhanced = self.create_derived_metrics(df_clean)
        
        # Step 4: Calculate rolling metrics
        df_final = self.calculate_rolling_metrics(df_enhanced)
        
        # Step 5: Create aggregated datasets
        aggregated_data = self.create_aggregated_datasets(df_final)
        
        # Step 6: Create Power BI ready datasets
        powerbi_data = self.create_powerbi_ready_datasets(df_final)
        
        # Step 7: Generate data quality report
        quality_report = self.generate_data_quality_report(df_final)
        
        # Step 8: Export all datasets
        all_datasets = {
            'processed_main_data': df_final,
            **aggregated_data,
            **powerbi_data
        }
        
        self.export_processed_data(all_datasets, 'csv')
        self.export_processed_data(powerbi_data, 'excel')  # Excel for Power BI
        
        # Save quality report
        import json
        report_file = f"{self.data_dir}/processed/data_quality_report.json"
        with open(report_file, 'w') as f:
            json.dump(quality_report, f, indent=2, default=str)
        logger.info(f"Data quality report saved to {report_file}")
        
        logger.info("=== Data Processing Pipeline Completed ===")
        logger.info(f"Processed main dataset: {len(df_final)} rows, {len(df_final.columns)} columns")
        logger.info(f"Created {len(all_datasets)} datasets for analysis")
        
        return df_final, all_datasets

def main():
    """Main execution function"""
    processor = CovidDataProcessor()
    
    # Run the complete processing pipeline
    processed_df, all_datasets = processor.process_full_pipeline()
    
    # Print summary statistics
    print("\n=== Processing Summary ===")
    print(f"Main processed dataset: {processed_df.shape}")
    print(f"Total datasets created: {len(all_datasets)}")
    
    print("\nDataset sizes:")
    for name, df in all_datasets.items():
        print(f"  {name}: {df.shape}")
    
    print(f"\nDate range: {processed_df['date'].min()} to {processed_df['date'].max()}")
    print(f"Countries: {processed_df['location'].nunique()}")
    
    # Show sample of key metrics
    print("\nSample of processed data:")
    key_columns = ['location', 'date', 'total_cases', 'new_cases', 'case_fatality_rate', 'pandemic_phase']
    available_columns = [col for col in key_columns if col in processed_df.columns]
    print(processed_df[available_columns].head(10))
    
    return processed_df, all_datasets

if __name__ == "__main__":
    processed_data, datasets = main()