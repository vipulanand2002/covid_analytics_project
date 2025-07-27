"""
COVID-19 Data Cleaning and Processing Script - India Focus
Cleans and transforms raw data for analysis and Power BI dashboards
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
import warnings

# Import configuration
try:
    from config import *
except ImportError:
    # Fallback if config import fails
    INDIAN_STATE_POPULATION = {
        'Uttar Pradesh': 231.0, 'Maharashtra': 112.4, 'Bihar': 104.1,
        'West Bengal': 91.3, 'Madhya Pradesh': 72.6, 'Tamil Nadu': 72.1,
        'Rajasthan': 68.5, 'Karnataka': 61.1, 'Gujarat': 60.4,
        'Andhra Pradesh': 49.4, 'Odisha': 42.0, 'Telangana': 35.0,
        'Kerala': 33.4, 'Jharkhand': 33.0, 'Assam': 31.2,
        'Punjab': 27.7, 'Chhattisgarh': 25.5, 'Haryana': 25.4,
        'Delhi': 16.8, 'Jammu and Kashmir': 12.5
    }
    
    MAJOR_COUNTRIES = ['India', 'China', 'United States', 'Brazil', 'Russia',
                      'United Kingdom', 'Germany', 'France', 'Italy',
                      'Japan', 'South Korea', 'Indonesia', 'Pakistan']
    
    REGIONS = {
        'South Asia': ['India', 'Pakistan', 'Bangladesh', 'Sri Lanka', 'Nepal', 'Bhutan', 'Maldives']
    }

warnings.filterwarnings('ignore')

class CovidDataCleaner:
    def __init__(self, raw_data_path="./data/raw/", processed_data_path="./data/processed/"):
        self.raw_data_path = raw_data_path
        self.processed_data_path = processed_data_path
        
        # Create processed data directory
        os.makedirs(processed_data_path, exist_ok=True)
        
        print(f"CovidDataCleaner initialized.")
        print(f"Raw data source: {os.path.abspath(raw_data_path)}")
        print(f"Processed data output: {os.path.abspath(processed_data_path)}")
    
    # =============================================================================
    # INDIAN DATA CLEANING
    # =============================================================================
    
    def clean_india_national_data(self):
        """Clean and process Indian national time series data"""
        
        print("🇮🇳 Cleaning Indian national time series data...")
        
        try:
            # Load national time series
            df = pd.read_csv(os.path.join(self.raw_data_path, "india_national_timeseries.csv"))
            
            # Standardize date column
            df['date'] = pd.to_datetime(df['Date_YMD'], errors='coerce')
            
            # Clean and standardize column names
            column_mapping = {
                'Daily Confirmed': 'daily_cases',
                'Total Confirmed': 'total_cases',
                'Daily Recovered': 'daily_recovered',
                'Total Recovered': 'total_recovered',
                'Daily Deceased': 'daily_deaths',
                'Total Deceased': 'total_deaths'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Add country column for consistency with global data
            df['location'] = 'India'
            df['iso_code'] = 'IND'
            
            # Calculate additional metrics
            df['active_cases'] = df['total_cases'] - df['total_recovered'] - df['total_deaths']
            df['recovery_rate'] = (df['total_recovered'] / df['total_cases'] * 100).round(2)
            df['fatality_rate'] = (df['total_deaths'] / df['total_cases'] * 100).round(2)
            
            # Calculate 7-day rolling averages
            df['daily_cases_7day_avg'] = df['daily_cases'].rolling(window=7, center=True).mean().round(0)
            df['daily_deaths_7day_avg'] = df['daily_deaths'].rolling(window=7, center=True).mean().round(1)
            
            # Select final columns
            final_columns = [
                'date', 'location', 'iso_code', 'daily_cases', 'total_cases',
                'daily_deaths', 'total_deaths', 'daily_recovered', 'total_recovered',
                'active_cases', 'recovery_rate', 'fatality_rate',
                'daily_cases_7day_avg', 'daily_deaths_7day_avg'
            ]
            
            df_clean = df[final_columns].copy()
            df_clean = df_clean.dropna(subset=['date']).sort_values('date')
            
            # Save cleaned data
            output_path = os.path.join(self.processed_data_path, "india_national_cleaned.csv")
            df_clean.to_csv(output_path, index=False)
            
            print(f"   ✅ National data cleaned: {len(df_clean)} days")
            print(f"   📅 Date range: {df_clean['date'].min().date()} to {df_clean['date'].max().date()}")
            
            return df_clean
            
        except Exception as e:
            print(f"   ❌ Error cleaning national data: {str(e)}")
            return None
    
    def clean_india_states_data(self):
        """Clean and process Indian state-wise data"""
        
        print("🏛️ Cleaning Indian state-wise data...")
        
        try:
            # Load current state data
            df_current = pd.read_csv(os.path.join(self.raw_data_path, "india_states_current.csv"))
            
            # Show available columns for debugging
            print(f"   📋 Available columns: {list(df_current.columns)}")
            
            # Clean state names
            df_current['state_clean'] = df_current['State'].str.strip()
            
            # Remove summary rows and invalid entries
            summary_rows = ['Total', 'State Unassigned', 'Unassigned', 'Unknown']
            df_current = df_current[~df_current['state_clean'].isin(summary_rows)]
            
            # Standardize column names to match expected format
            column_mapping = {
                'state_clean': 'state',
                'Confirmed': 'total_cases',
                'Deaths': 'total_deaths',
                'Recovered': 'total_recovered',
                'Active': 'active_cases',
                'Last_Updated_Time': 'last_updated_time',
                'Delta_Confirmed': 'daily_cases',
                'Delta_Recovered': 'daily_recovered',
                'Delta_Deaths': 'daily_deaths',
                'State_code': 'state_code'
            }
            
            df_current = df_current.rename(columns=column_mapping)
            
            # Convert numeric columns and handle any text/missing values
            numeric_columns = ['total_cases', 'total_deaths', 'total_recovered', 'active_cases', 
                             'daily_cases', 'daily_recovered', 'daily_deaths']
            
            for col in numeric_columns:
                if col in df_current.columns:
                    df_current[col] = pd.to_numeric(df_current[col], errors='coerce').fillna(0)
            
            # Define population data directly (2021 estimates in millions)
            state_population = {
                'Uttar Pradesh': 231.0, 'Maharashtra': 112.4, 'Bihar': 104.1,
                'West Bengal': 91.3, 'Madhya Pradesh': 72.6, 'Tamil Nadu': 72.1,
                'Rajasthan': 68.5, 'Karnataka': 61.1, 'Gujarat': 60.4,
                'Andhra Pradesh': 49.4, 'Odisha': 42.0, 'Telangana': 35.0,
                'Kerala': 33.4, 'Jharkhand': 33.0, 'Assam': 31.2,
                'Punjab': 27.7, 'Chhattisgarh': 25.5, 'Haryana': 25.4,
                'Delhi': 16.8, 'Jammu and Kashmir': 12.5, 'Uttarakhand': 10.1,
                'Himachal Pradesh': 6.9, 'Tripura': 3.7, 'Meghalaya': 3.0,
                'Manipur': 2.9, 'Nagaland': 2.0, 'Goa': 1.5, 'Arunachal Pradesh': 1.4,
                'Mizoram': 1.1, 'Sikkim': 0.6, 'Andaman and Nicobar Islands': 0.4,
                'Chandigarh': 1.1, 'Dadra and Nagar Haveli and Daman and Diu': 0.6,
                'Lakshadweep': 0.06, 'Puducherry': 1.2, 'Ladakh': 0.3
            }
            
            # Map population data, using default values for any missing states
            df_current['population'] = df_current['state'].map(state_population)
            
            # For states without population data, use estimated values based on total cases
            df_current['population'] = df_current['population'].fillna(1.0)  # Default 1 million for small UTs
            
            # Calculate per capita metrics (per 100k population)
            df_current['cases_per_100k'] = (df_current['total_cases'] / df_current['population'] * 100).round(1)
            df_current['deaths_per_100k'] = (df_current['total_deaths'] / df_current['population'] * 100).round(1)
            
            # Calculate rates (handle division by zero)
            df_current['recovery_rate'] = 0
            df_current['fatality_rate'] = 0
            df_current['active_rate'] = 0
            
            # Calculate rates only where total_cases > 0
            mask = df_current['total_cases'] > 0
            df_current.loc[mask, 'recovery_rate'] = (df_current.loc[mask, 'total_recovered'] / df_current.loc[mask, 'total_cases'] * 100).round(2)
            df_current.loc[mask, 'fatality_rate'] = (df_current.loc[mask, 'total_deaths'] / df_current.loc[mask, 'total_cases'] * 100).round(2)
            df_current.loc[mask, 'active_rate'] = (df_current.loc[mask, 'active_cases'] / df_current.loc[mask, 'total_cases'] * 100).round(2)
            
            # Add metadata
            df_current['country'] = 'India'
            df_current['last_updated'] = datetime.now().strftime('%Y-%m-%d')
            
            # Select final columns
            final_columns = [
                'state', 'country', 'state_code', 'total_cases', 'total_deaths', 'total_recovered', 'active_cases',
                'daily_cases', 'daily_deaths', 'daily_recovered',
                'population', 'cases_per_100k', 'deaths_per_100k',
                'recovery_rate', 'fatality_rate', 'active_rate', 'last_updated'
            ]
            
            # Only include columns that exist
            available_final_columns = [col for col in final_columns if col in df_current.columns]
            df_clean = df_current[available_final_columns].copy()
            
            # Sort by total cases (descending)
            df_clean = df_clean.sort_values('total_cases', ascending=False)
            
            # Save cleaned data
            output_path = os.path.join(self.processed_data_path, "india_states_cleaned.csv")
            df_clean.to_csv(output_path, index=False)
            
            print(f"   ✅ State data cleaned: {len(df_clean)} states/UTs")
            if len(df_clean) > 0:
                print(f"   🏆 Top 3 states by cases: {', '.join(df_clean.head(3)['state'].tolist())}")
                print(f"   📊 Total cases across all states: {df_clean['total_cases'].sum():,}")
            
            return df_clean
            
        except Exception as e:
            print(f"   ❌ Error cleaning state data: {str(e)}")
            import traceback
            print(f"   🔍 Detailed error: {traceback.format_exc()}")
            return None
    
    def clean_india_districts_data(self):
        """Clean and process Indian district-wise data"""
        
        print("🏘️ Cleaning Indian district-wise data...")
        
        try:
            # Load district data
            df = pd.read_csv(os.path.join(self.raw_data_path, "india_districts.csv"))
            
            # Check what columns are actually available
            print(f"   📋 Available columns: {list(df.columns)}")
            
            # Clean and standardize
            df['district_clean'] = df['District'].str.strip()
            df['state_clean'] = df['State'].str.strip()
            
            # Remove unknown/unassigned districts
            exclude_districts = ['Unknown', 'Unassigned', 'Other State', 'Airport Quarantine', 'Railway Quarantine']
            df = df[~df['district_clean'].isin(exclude_districts)]
            
            # Handle different possible column names for cases/deaths/recovered
            case_columns = ['Confirmed', 'confirmed', 'Cases', 'cases', 'Total_Confirmed']
            death_columns = ['Deaths', 'deaths', 'Deceased', 'deceased', 'Total_Deaths']
            recovered_columns = ['Recovered', 'recovered', 'Total_Recovered']
            active_columns = ['Active', 'active', 'Active_Cases']
            
            # Find the correct column names
            total_cases_col = None
            total_deaths_col = None
            total_recovered_col = None
            active_cases_col = None
            
            for col in case_columns:
                if col in df.columns:
                    total_cases_col = col
                    break
            
            for col in death_columns:
                if col in df.columns:
                    total_deaths_col = col
                    break
                    
            for col in recovered_columns:
                if col in df.columns:
                    total_recovered_col = col
                    break
                    
            for col in active_columns:
                if col in df.columns:
                    active_cases_col = col
                    break
            
            # Standardize column names
            column_mapping = {
                'state_clean': 'state',
                'district_clean': 'district'
            }
            
            if total_cases_col:
                column_mapping[total_cases_col] = 'total_cases'
            if total_deaths_col:
                column_mapping[total_deaths_col] = 'total_deaths'
            if total_recovered_col:
                column_mapping[total_recovered_col] = 'total_recovered'
            if active_cases_col:
                column_mapping[active_cases_col] = 'active_cases'
            
            df = df.rename(columns=column_mapping)
            
            # Fill missing columns with 0 if they don't exist
            required_columns = ['total_cases', 'total_deaths', 'total_recovered', 'active_cases']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = 0
            
            # Convert to numeric, replacing any non-numeric values with 0
            for col in required_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Calculate district metrics (only if we have valid data)
            df['recovery_rate'] = 0
            df['fatality_rate'] = 0
            
            # Calculate rates only where total_cases > 0
            mask = df['total_cases'] > 0
            df.loc[mask, 'recovery_rate'] = (df.loc[mask, 'total_recovered'] / df.loc[mask, 'total_cases'] * 100).round(2)
            df.loc[mask, 'fatality_rate'] = (df.loc[mask, 'total_deaths'] / df.loc[mask, 'total_cases'] * 100).round(2)
            
            # Add ranking within state
            df['state_rank'] = df.groupby('state')['total_cases'].rank(ascending=False, method='dense').astype(int)
            
            # Add metadata
            df['country'] = 'India'
            df['last_updated'] = datetime.now().strftime('%Y-%m-%d')
            
            # Select final columns
            final_columns = [
                'state', 'district', 'country', 'total_cases', 'total_deaths', 
                'total_recovered', 'active_cases', 'recovery_rate', 'fatality_rate',
                'state_rank', 'last_updated'
            ]
            
            df_clean = df[final_columns].copy()
            df_clean = df_clean.sort_values(['state', 'total_cases'], ascending=[True, False])
            
            # Save cleaned data
            output_path = os.path.join(self.processed_data_path, "india_districts_cleaned.csv")
            df_clean.to_csv(output_path, index=False)
            
            print(f"   ✅ District data cleaned: {len(df_clean)} districts")
            if len(df_clean) > 0:
                print(f"   🏆 Top district: {df_clean.iloc[0]['district']}, {df_clean.iloc[0]['state']}")
            print(f"   📊 States covered: {df_clean['state'].nunique()}")
            
            return df_clean
            
        except Exception as e:
            print(f"   ❌ Error cleaning district data: {str(e)}")
            return None
    
    # =============================================================================
    # GLOBAL DATA CLEANING
    # =============================================================================
    
    def clean_johns_hopkins_data(self):
        """Clean and process Johns Hopkins global data"""
        
        print("📊 Cleaning Johns Hopkins global data...")
        
        try:
            # Load confirmed cases
            df_confirmed = pd.read_csv(os.path.join(self.raw_data_path, "jhu_confirmed_global.csv"))
            df_deaths = pd.read_csv(os.path.join(self.raw_data_path, "jhu_deaths_global.csv"))
            
            # Function to transform JHU data to long format
            def transform_jhu_data(df, value_name):
                # Melt the data to long format
                id_vars = ['Province/State', 'Country/Region', 'Lat', 'Long']
                df_long = df.melt(id_vars=id_vars, var_name='date_str', value_name=value_name)
                
                # Clean date
                df_long['date'] = pd.to_datetime(df_long['date_str'], format='%m/%d/%y', errors='coerce')
                
                # Clean country names
                df_long['country'] = df_long['Country/Region'].str.strip()
                
                # Aggregate by country (sum provinces/states)
                df_agg = df_long.groupby(['country', 'date'])[value_name].sum().reset_index()
                
                return df_agg
            
            # Transform both datasets
            df_cases = transform_jhu_data(df_confirmed, 'total_cases')
            df_deaths_data = transform_jhu_data(df_deaths, 'total_deaths')
            
            # Merge cases and deaths
            df_global = pd.merge(df_cases, df_deaths_data, on=['country', 'date'], how='outer')
            
            # Focus on major countries including India and neighbors
            focus_countries = MAJOR_COUNTRIES + REGIONS['South Asia']
            df_global = df_global[df_global['country'].isin(focus_countries)]
            
            # Calculate daily values
            df_global = df_global.sort_values(['country', 'date'])
            df_global['daily_cases'] = df_global.groupby('country')['total_cases'].diff().fillna(0)
            df_global['daily_deaths'] = df_global.groupby('country')['total_deaths'].diff().fillna(0)
            
            # Calculate 7-day averages
            df_global['daily_cases_7day_avg'] = df_global.groupby('country')['daily_cases'].transform(
                lambda x: x.rolling(window=7, center=True).mean()
            ).round(0)
            
            # Calculate rates
            df_global['fatality_rate'] = (df_global['total_deaths'] / df_global['total_cases'] * 100).round(2)
            
            # Clean negative daily values (data corrections)
            df_global['daily_cases'] = df_global['daily_cases'].clip(lower=0)
            df_global['daily_deaths'] = df_global['daily_deaths'].clip(lower=0)
            
            # Save cleaned data
            output_path = os.path.join(self.processed_data_path, "global_jhu_cleaned.csv")
            df_global.to_csv(output_path, index=False)
            
            print(f"   ✅ Global JHU data cleaned: {len(df_global)} records")
            print(f"   🌍 Countries: {df_global['country'].nunique()}")
            print(f"   🇮🇳 India records: {len(df_global[df_global['country'] == 'India'])}")
            
            return df_global
            
        except Exception as e:
            print(f"   ❌ Error cleaning JHU data: {str(e)}")
            return None
    
    def clean_owid_data(self):
        """Clean and process Our World in Data complete dataset"""
        
        print("📈 Cleaning Our World in Data comprehensive dataset...")
        
        try:
            # Load OWID complete data
            df = pd.read_csv(os.path.join(self.raw_data_path, "owid_complete_covid_data.csv"))
            
            # Focus on countries of interest
            focus_countries = MAJOR_COUNTRIES + REGIONS['South Asia'] + ['World']
            df = df[df['location'].isin(focus_countries)]
            
            # Clean date
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
            
            # Select key columns for analysis
            key_columns = [
                'iso_code', 'location', 'date',
                # Cases and deaths
                'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
                'total_cases_per_million', 'new_cases_per_million',
                'total_deaths_per_million', 'new_deaths_per_million',
                # Testing
                'total_tests', 'new_tests', 'positive_rate',
                'tests_per_case', 'total_tests_per_thousand',
                # Vaccinations
                'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
                'total_boosters', 'new_vaccinations', 
                'total_vaccinations_per_hundred', 'people_vaccinated_per_hundred',
                'people_fully_vaccinated_per_hundred',
                # Hospital & ICU
                'hosp_patients', 'icu_patients', 'hosp_patients_per_million',
                'icu_patients_per_million',
                # Policy responses
                'stringency_index',
                # Demographics and development
                'population', 'population_density', 'median_age',
                'gdp_per_capita', 'human_development_index'
            ]
            
            # Keep only available columns
            available_columns = [col for col in key_columns if col in df.columns]
            df_clean = df[available_columns].copy()
            
            # Calculate additional metrics
            df_clean['case_fatality_rate'] = (df_clean['total_deaths'] / df_clean['total_cases'] * 100).round(2)
            
            # Calculate 7-day rolling averages for new cases and deaths
            df_clean = df_clean.sort_values(['location', 'date'])
            df_clean['new_cases_7day_avg'] = df_clean.groupby('location')['new_cases'].transform(
                lambda x: x.rolling(window=7, center=True).mean()
            ).round(0)
            
            df_clean['new_deaths_7day_avg'] = df_clean.groupby('location')['new_deaths'].transform(
                lambda x: x.rolling(window=7, center=True).mean()
            ).round(1)
            
            # Clean negative values
            numeric_columns = df_clean.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                if 'new_' in col or 'daily_' in col:
                    df_clean[col] = df_clean[col].clip(lower=0)
            
            # Save cleaned data
            output_path = os.path.join(self.processed_data_path, "global_owid_cleaned.csv")
            df_clean.to_csv(output_path, index=False)
            
            # Save India-only data separately
            df_india = df_clean[df_clean['location'] == 'India'].copy()
            india_output_path = os.path.join(self.processed_data_path, "india_owid_cleaned.csv")
            df_india.to_csv(india_output_path, index=False)
            
            print(f"   ✅ OWID data cleaned: {len(df_clean)} records")
            print(f"   🌍 Countries: {df_clean['location'].nunique()}")
            print(f"   🇮🇳 India records: {len(df_india)}")
            print(f"   📊 Metrics available: {len(available_columns)}")
            
            return df_clean
            
        except Exception as e:
            print(f"   ❌ Error cleaning OWID data: {str(e)}")
            return None
    
    def clean_government_response_data(self):
        """Clean and process Oxford Government Response Tracker data"""
        
        print("🏛️ Cleaning government response data...")
        
        try:
            # Load government response data
            df = pd.read_csv(os.path.join(self.raw_data_path, "oxford_government_response.csv"))
            
            # Focus on countries of interest
            focus_countries = MAJOR_COUNTRIES + REGIONS['South Asia']
            df = df[df['CountryName'].isin(focus_countries)]
            
            # Clean date
            df['Date'] = df['Date'].astype(str)
            df['date'] = pd.to_datetime(df['Date'], format='%Y%m%d', errors='coerce')
            df = df.dropna(subset=['date'])
            
            # Select key policy indicators
            policy_columns = [
                'CountryName', 'CountryCode', 'date',
                'StringencyIndex_Average', 'StringencyIndex_Average_ForDisplay',
                'GovernmentResponseIndex_Average', 'ContainmentHealthIndex_Average',
                'EconomicSupportIndex', 'C1_School closing', 'C2_Workplace closing',
                'C3_Cancel public events', 'C4_Restrictions on gatherings',
                'C5_Close public transport', 'C6_Stay at home requirements',
                'C7_Restrictions on internal movement', 'C8_International travel controls',
                'H1_Public information campaigns', 'H2_Testing policy', 'H3_Contact tracing'
            ]
            
            # Keep only available columns
            available_columns = [col for col in policy_columns if col in df.columns]
            df_clean = df[available_columns].copy()
            
            # Standardize column names
            df_clean = df_clean.rename(columns={
                'CountryName': 'country',
                'CountryCode': 'country_code',
                'StringencyIndex_Average': 'stringency_index'
            })
            
            # Save cleaned data
            output_path = os.path.join(self.processed_data_path, "government_response_cleaned.csv")
            df_clean.to_csv(output_path, index=False)
            
            # Save India-only data
            df_india = df_clean[df_clean['country'] == 'India'].copy()
            india_output_path = os.path.join(self.processed_data_path, "india_government_response_cleaned.csv")
            df_india.to_csv(india_output_path, index=False)
            
            print(f"   ✅ Government response data cleaned: {len(df_clean)} records")
            print(f"   🌍 Countries: {df_clean['country'].nunique()}")
            print(f"   🇮🇳 India policy records: {len(df_india)}")
            
            return df_clean
            
        except Exception as e:
            print(f"   ❌ Error cleaning government response data: {str(e)}")
            return None
    
    # =============================================================================
    # POWER BI OPTIMIZED DATASETS
    # =============================================================================
    
    def create_powerbi_datasets(self):
        """Create optimized datasets specifically for Power BI dashboards"""
        
        print("📊 Creating Power BI optimized datasets...")
        
        # Create Power BI subfolder
        powerbi_path = os.path.join(self.processed_data_path, "powerbi_ready")
        os.makedirs(powerbi_path, exist_ok=True)
        
        try:
            # 1. India Summary Dashboard Data
            print("   📈 Creating India summary dashboard data...")
            
            # Load cleaned India data
            india_national_path = os.path.join(self.processed_data_path, "india_national_cleaned.csv")
            india_states_path = os.path.join(self.processed_data_path, "india_states_cleaned.csv")
            
            if os.path.exists(india_national_path):
                india_national = pd.read_csv(india_national_path)
                
                # Create summary metrics
                latest_national = india_national.iloc[-1]
                
                summary_data = {
                    'metric': ['Total Cases', 'Total Deaths', 'Total Recovered', 'Active Cases', 
                              'Recovery Rate (%)', 'Fatality Rate (%)'],
                    'value': [
                        latest_national['total_cases'],
                        latest_national['total_deaths'], 
                        latest_national['total_recovered'],
                        latest_national['active_cases'],
                        latest_national['recovery_rate'],
                        latest_national['fatality_rate']
                    ],
                    'last_updated': [latest_national['date']] * 6
                }
                
                # Add states count if available
                if os.path.exists(india_states_path):
                    india_states = pd.read_csv(india_states_path)
                    summary_data['metric'].append('States Affected')
                    summary_data['value'].append(len(india_states))
                    summary_data['last_updated'].append(latest_national['date'])
                
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_csv(os.path.join(powerbi_path, "india_summary_metrics.csv"), index=False)
                
                # 3. Time Series for Trends
                print("   📅 Creating time series for trend analysis...")
                
                # Add month-year for Power BI date hierarchy
                india_national['year'] = pd.to_datetime(india_national['date']).dt.year
                india_national['month'] = pd.to_datetime(india_national['date']).dt.month
                india_national['month_year'] = pd.to_datetime(india_national['date']).dt.to_period('M').astype(str)
                
                india_national.to_csv(os.path.join(powerbi_path, "india_daily_trends.csv"), index=False)
            
            # 2. State Performance Dashboard (if available)
            if os.path.exists(india_states_path):
                print("   🏛️ Creating state performance dashboard data...")
                
                india_states = pd.read_csv(india_states_path)
                
                # Add performance categories
                india_states['performance_category'] = pd.cut(
                    india_states['recovery_rate'], 
                    bins=[0, 85, 92, 97, 100], 
                    labels=['Needs Improvement', 'Average', 'Good', 'Excellent']
                )
                
                india_states.to_csv(os.path.join(powerbi_path, "india_states_performance.csv"), index=False)
            
            # 4. Global Comparison Ready Data
            print("   🌍 Creating global comparison data...")
            
            # Load and prepare global data
            global_owid_path = os.path.join(self.processed_data_path, "global_owid_cleaned.csv")
            
            if os.path.exists(global_owid_path):
                global_owid = pd.read_csv(global_owid_path)
                
                # Get latest data for each country
                latest_global = global_owid.groupby('location').last().reset_index()
                
                # Focus on South Asian countries + major economies
                comparison_countries = ['India'] + REGIONS['South Asia'] + ['United States', 'China', 'Brazil', 'United Kingdom']
                latest_global = latest_global[latest_global['location'].isin(comparison_countries)]
                
                # Select key metrics for comparison
                comparison_columns = [
                    'location', 'total_cases', 'total_deaths', 'total_cases_per_million',
                    'total_deaths_per_million', 'people_fully_vaccinated_per_hundred',
                    'gdp_per_capita', 'human_development_index', 'population'
                ]
                
                available_comparison_columns = [col for col in comparison_columns if col in latest_global.columns]
                df_comparison = latest_global[available_comparison_columns].copy()
                
                df_comparison.to_csv(os.path.join(powerbi_path, "global_comparison.csv"), index=False)
            
            # List created files
            created_files = [f for f in os.listdir(powerbi_path) if f.endswith('.csv')]
            
            print(f"   ✅ Power BI datasets created in: {powerbi_path}")
            print(f"   📁 Files created: {', '.join(created_files)}")
            
        except Exception as e:
            print(f"   ❌ Error creating Power BI datasets: {str(e)}")
            import traceback
            print(f"   🔍 Detailed error: {traceback.format_exc()}")
    
    # =============================================================================
    # MAIN CLEANING FUNCTION
    # =============================================================================
    
    def clean_all_data(self):
        """Run all data cleaning functions"""
        
        print("=" * 60)
        print("🧹 STARTING COVID-19 DATA CLEANING - INDIA FOCUS")
        print("=" * 60)
        print(f"📅 Cleaning started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Clean Indian data first
        india_national = self.clean_india_national_data()
        india_states = self.clean_india_states_data()
        india_districts = self.clean_india_districts_data()
        
        print()
        
        # Clean global data for comparison
        global_jhu = self.clean_johns_hopkins_data()
        global_owid = self.clean_owid_data()
        govt_response = self.clean_government_response_data()
        
        print()
        
        # Create Power BI ready datasets
        self.create_powerbi_datasets()
        
        print()
        print("=" * 60)
        print("✅ DATA CLEANING COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
        # Summary of processed files
        processed_files = [f for f in os.listdir(self.processed_data_path) if f.endswith('.csv')]
        powerbi_files = []
        powerbi_folder = os.path.join(self.processed_data_path, "powerbi_ready")
        if os.path.exists(powerbi_folder):
            powerbi_files = [f for f in os.listdir(powerbi_folder) if f.endswith('.csv')]
        
        print(f"📁 Processed files created: {len(processed_files)}")
        for file in sorted(processed_files):
            print(f"   📄 {file}")
        
        if powerbi_files:
            print(f"\n📊 Power BI ready files: {len(powerbi_files)}")
            for file in sorted(powerbi_files):
                print(f"   📄 {file}")
        
        print(f"\n🎉 All cleaned data saved to: {os.path.abspath(self.processed_data_path)}")
        print("🚀 Ready for analysis and Power BI dashboard creation!")

# =============================================================================
# USAGE EXAMPLE
# =============================================================================

if __name__ == "__main__":
    # Initialize cleaner
    cleaner = CovidDataCleaner()
    
    # Clean all data
    cleaner.clean_all_data()
    
    print("\n💡 Next Steps:")
    print("1. Load processed data into Power BI")
    print("2. Create Jupyter notebooks for exploratory analysis")
    print("3. Build interactive dashboards")
    print("4. Generate insights and reports")