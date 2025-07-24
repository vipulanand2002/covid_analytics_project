"""
COVID-19 Data Collection Script - India Focus
Collects global data with special emphasis on Indian data sources
"""

import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta
import time

class CovidDataCollector:
    def __init__(self, data_path="../data/raw/"):
        self.data_path = data_path
        self.base_jhu_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
        self.owid_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/"
        
        # Create data directories if they don't exist
        os.makedirs(data_path, exist_ok=True)
        
        print(f"CovidDataCollector initialized. Data will be saved to: {data_path}")
    
    # =============================================================================
    # INDIAN DATA COLLECTION (Updated Working APIs)
    # =============================================================================
    
    def collect_india_specific_data(self):
        """Collect detailed Indian COVID-19 data from working data sources"""
        
        print("🇮🇳 Collecting India-specific data from updated sources...")
        
        # National level time series data (CSV format - more reliable)
        try:
            print("   📊 Fetching national time series data...")
            url = "https://data.incovid19.org/csv/latest/case_time_series.csv"
            df_national = pd.read_csv(url)
            
            # Save national time series
            output_path = os.path.join(self.data_path, "india_national_timeseries.csv")
            df_national.to_csv(output_path, index=False)
            
            print(f"   ✅ National time series: {len(df_national)} days of data")
            
        except Exception as e:
            print(f"   ❌ Failed to collect national time series: {str(e)}")
        
        # State-wise data
        try:
            print("   🏛️ Fetching state-wise data...")
            url = "https://data.incovid19.org/csv/latest/state_wise.csv"
            df_states = pd.read_csv(url)
            
            # Save states data
            output_path = os.path.join(self.data_path, "india_states_current.csv")
            df_states.to_csv(output_path, index=False)
            
            print(f"   ✅ State data: {len(df_states)} states/UTs")
            
        except Exception as e:
            print(f"   ❌ Failed to collect state data: {str(e)}")
        
        # District-wise data
        try:
            print("   🏘️ Fetching district-wise data...")
            url = "https://data.incovid19.org/csv/latest/district_wise.csv"
            df_districts = pd.read_csv(url)
            
            # Save district data
            output_path = os.path.join(self.data_path, "india_districts.csv")
            df_districts.to_csv(output_path, index=False)
            
            print(f"   ✅ District data: {len(df_districts)} districts across India")
            
        except Exception as e:
            print(f"   ❌ Failed to collect district data: {str(e)}")
        
        # Alternative API for additional state data
        try:
            print("   📈 Fetching additional state data from alternative source...")
            url = "https://api.rootnet.in/covid19-in/stats/latest"
            response = requests.get(url)
            data = response.json()
            
            if 'data' in data:
                # Convert to DataFrame
                regional_data = data['data']['regional']
                df_alt_states = pd.DataFrame(regional_data)
                
                # Save alternative state data
                output_path = os.path.join(self.data_path, "india_states_alternative.csv")
                df_alt_states.to_csv(output_path, index=False)
                
                print(f"   ✅ Alternative state data: {len(df_alt_states)} states")
            
        except Exception as e:
            print(f"   ⚠️ Alternative state data unavailable: {str(e)}")
        
        # Try to get historical state data
        try:
            print("   📅 Fetching historical state data...")
            url = "https://api.rootnet.in/covid19-in/unofficial/covid19india.org/statewise/history"
            response = requests.get(url)
            data = response.json()
            
            if 'data' in data:
                # Convert to DataFrame  
                df_history = pd.DataFrame(data['data'])
                
                # Save historical data
                output_path = os.path.join(self.data_path, "india_states_history.csv")
                df_history.to_csv(output_path, index=False)
                
                print(f"   ✅ Historical state data: {len(df_history)} records")
            
        except Exception as e:
            print(f"   ⚠️ Historical data unavailable: {str(e)}")
        
        print("🇮🇳 Indian data collection completed!\n")

    # =============================================================================
    # ENHANCED GLOBAL DATA WITH INDIA FOCUS
    # =============================================================================
    
    def collect_jhu_data(self):
        """Collect Johns Hopkins time series data with focus on India and comparison countries"""
        
        datasets = {
            'confirmed_global': 'time_series_covid19_confirmed_global.csv',
            'deaths_global': 'time_series_covid19_deaths_global.csv',
            'confirmed_us': 'time_series_covid19_confirmed_US.csv',
            'deaths_us': 'time_series_covid19_deaths_US.csv'
        }
        
        print("🔄 Collecting Johns Hopkins global data (India focus)...")
        
        for dataset_name, filename in datasets.items():
            try:
                url = self.base_jhu_url + filename
                df = pd.read_csv(url)
                
                # Filter for major countries if it's global data
                if 'global' in dataset_name:
                    # Keep all data but highlight India's position
                    india_data = df[df['Country/Region'] == 'India']
                    if not india_data.empty:
                        print(f"   🇮🇳 India data found in {dataset_name}")
                
                # Save raw data
                output_path = os.path.join(self.data_path, f"jhu_{dataset_name}.csv")
                df.to_csv(output_path, index=False)
                
                print(f"   ✅ {dataset_name}: {df.shape[0]} rows, {df.shape[1]} columns")
                time.sleep(1)  # Be respectful to the server
                
            except Exception as e:
                print(f"   ❌ Failed to collect {dataset_name}: {str(e)}")
        
        print("📊 Johns Hopkins data collection completed!\n")
    
    # =============================================================================
    # VACCINATION DATA (Our World in Data)
    # =============================================================================
    
    def collect_vaccination_data(self):
        """Collect vaccination data from Our World in Data"""
        
        # Updated vaccination dataset URLs (some may have changed)
        vaccination_datasets = {
            'vaccinations': 'vaccinations.csv',
            'vaccinations_by_manufacturer': 'vaccinations-by-manufacturer.csv'
        }
        
        print("🔄 Collecting vaccination data...")
        
        for dataset_name, filename in vaccination_datasets.items():
            try:
                url = self.owid_url + filename
                df = pd.read_csv(url)
                
                # Check for India data specifically
                if 'location' in df.columns:
                    india_data = df[df['location'] == 'India']
                    if not india_data.empty:
                        print(f"   🇮🇳 India vaccination records found: {len(india_data)}")
                
                # Save raw data
                output_path = os.path.join(self.data_path, f"owid_{dataset_name}.csv")
                df.to_csv(output_path, index=False)
                
                print(f"   ✅ {dataset_name}: {df.shape[0]} rows, {df.shape[1]} columns")
                time.sleep(1)
                
            except requests.exceptions.HTTPError as e:
                if "404" in str(e):
                    print(f"   ⚠️ {dataset_name} URL may have changed (404 error)")
                    print(f"   💡 Will try to get vaccination data from complete OWID dataset")
                else:
                    print(f"   ❌ Failed to collect {dataset_name}: {str(e)}")
            except Exception as e:
                print(f"   ❌ Failed to collect {dataset_name}: {str(e)}")
        
        print("💉 Vaccination data collection completed!\n")
    
    # =============================================================================
    # COMPREHENSIVE COVID DATA (Our World in Data)
    # =============================================================================
    
    def collect_owid_complete_data(self):
        """Collect complete COVID dataset from Our World in Data with India analysis"""
        
        print("🔄 Collecting comprehensive Our World in Data dataset...")
        
        try:
            url = self.owid_url + "owid-covid-data.csv"
            df = pd.read_csv(url)
            
            # Check India's data specifically
            india_data = df[df['location'] == 'India']
            
            print(f"📈 Global dataset shape: {df.shape}")
            print(f"📅 Date range: {df['date'].min()} to {df['date'].max()}")
            print(f"🌍 Countries: {df['location'].nunique()}")
            print(f"🇮🇳 India records: {len(india_data)} days of data")
            
            if not india_data.empty:
                print(f"🇮🇳 India date range: {india_data['date'].min()} to {india_data['date'].max()}")
            
            # Save raw data
            output_path = os.path.join(self.data_path, "owid_complete_covid_data.csv")
            df.to_csv(output_path, index=False)
            
            # Also save India-specific data separately for quick access
            if not india_data.empty:
                india_output_path = os.path.join(self.data_path, "owid_india_only.csv")
                india_data.to_csv(india_output_path, index=False)
                print(f"🇮🇳 India-only dataset saved separately")
            
            # Show available columns for reference
            print(f"📋 Available metrics ({len(df.columns)} total):")
            
            # Group columns by category for better understanding
            health_metrics = [col for col in df.columns if any(word in col.lower() 
                            for word in ['cases', 'deaths', 'tests', 'positive', 'hospital', 'icu'])]
            
            vaccine_metrics = [col for col in df.columns if any(word in col.lower() 
                             for word in ['vaccin', 'boost'])]
            
            economic_metrics = [col for col in df.columns if any(word in col.lower() 
                              for word in ['gdp', 'poverty', 'human_development', 'life_expectancy'])]
            
            policy_metrics = [col for col in df.columns if any(word in col.lower() 
                            for word in ['stringency', 'policy', 'government'])]
            
            print(f"   🏥 Health metrics: {len(health_metrics)}")
            print(f"   💉 Vaccine metrics: {len(vaccine_metrics)}")
            print(f"   💰 Economic metrics: {len(economic_metrics)}")
            print(f"   🏛️ Policy metrics: {len(policy_metrics)}")
            
            print("✅ Complete OWID data collection successful!\n")
            
        except Exception as e:
            print(f"❌ Failed to collect OWID complete data: {str(e)}\n")
    
    # =============================================================================
    # GOVERNMENT RESPONSE DATA (Oxford COVID-19 Government Response Tracker)
    # =============================================================================
    
    def collect_government_response_data(self):
        """Collect government response stringency data"""
        
        print("🔄 Collecting government response data...")
        
        try:
            # This data is included in OWID complete dataset, but we can also get it separately
            url = "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_nat_latest.csv"
            df = pd.read_csv(url)
            
            # Save raw data
            output_path = os.path.join(self.data_path, "oxford_government_response.csv")
            df.to_csv(output_path, index=False)
            
            print(f"✅ Government response data: {df.shape[0]} rows, {df.shape[1]} columns")
            print(f"📅 Date range: {df['Date'].min()} to {df['Date'].max()}")
            print(f"🌍 Countries: {df['CountryName'].nunique()}")
            
        except Exception as e:
            print(f"❌ Failed to collect government response data: {str(e)}")
        
        print("🏛️ Government response data collection completed!\n")
    
    # =============================================================================
    # POPULATION DATA (World Bank)
    # =============================================================================
    
    def collect_population_data(self):
        """Collect population data for per-capita calculations"""
        
        print("🔄 Collecting population reference data...")
        
        try:
            # Using a simple population dataset
            url = "https://raw.githubusercontent.com/datasets/population/master/data/population.csv"
            df = pd.read_csv(url)
            
            # Filter for most recent year and clean
            latest_year = df['Year'].max()
            df_latest = df[df['Year'] == latest_year].copy()
            
            # Save raw data
            output_path = os.path.join(self.data_path, "world_population.csv")
            df_latest.to_csv(output_path, index=False)
            
            print(f"✅ Population data: {df_latest.shape[0]} countries for year {latest_year}")
            
        except Exception as e:
            print(f"❌ Failed to collect population data: {str(e)}")
        
        print("👥 Population data collection completed!\n")
    
    # =============================================================================
    # MAIN COLLECTION FUNCTION
    # =============================================================================
    
    def collect_all_data(self):
        """Run all data collection functions with India focus"""
        
        print("=" * 60)
        print("🚀 STARTING COVID-19 DATA COLLECTION - INDIA FOCUS")
        print("=" * 60)
        print(f"📅 Collection started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 Primary focus: India and global comparisons")
        print()
        
        # Collect India-specific data first
        self.collect_india_specific_data()
        
        # Then collect global data for comparison
        self.collect_jhu_data()
        self.collect_vaccination_data()
        self.collect_owid_complete_data()
        self.collect_government_response_data()
        self.collect_population_data()
        
        print("=" * 60)
        print("✅ DATA COLLECTION COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
        # Summary of collected files
        files = os.listdir(self.data_path)
        csv_files = [f for f in files if f.endswith('.csv')]
        
        print(f"📁 Total files collected: {len(csv_files)}")
        
        # Separate India-specific and global files
        india_files = [f for f in csv_files if 'india' in f.lower()]
        global_files = [f for f in csv_files if 'india' not in f.lower()]
        
        if india_files:
            print(f"\n🇮🇳 India-specific files ({len(india_files)}):")
            for file in sorted(india_files):
                file_path = os.path.join(self.data_path, file)
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                print(f"   📄 {file} ({size_mb:.2f} MB)")
        
        if global_files:
            print(f"\n🌍 Global comparison files ({len(global_files)}):")
            for file in sorted(global_files):
                file_path = os.path.join(self.data_path, file)
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                print(f"   📄 {file} ({size_mb:.2f} MB)")
        
        print(f"\n🎉 All data saved to: {os.path.abspath(self.data_path)}")
        print("🔄 Ready for India-focused analysis!")

# =============================================================================
# USAGE EXAMPLE
# =============================================================================

if __name__ == "__main__":
    # Initialize collector
    collector = CovidDataCollector()
    
    # Collect all data with India focus
    collector.collect_all_data()
    
    print("\n💡 Next Steps:")
    print("1. Explore Indian state-wise trends")
    print("2. Compare India with South Asian neighbors")
    print("3. Analyze India's vaccination rollout")
    print("4. Create India-focused Power BI dashboard")
    print("5. Run: python data_cleaning.py")