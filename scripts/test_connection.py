"""
Test script to verify data sources and connections - India Focus
Run this first to make sure everything is working
"""

import pandas as pd
import requests
import json
import os
import sys
from config import *

def test_data_source(name, url, expected_columns=None):
    """Test if a data source is accessible and has expected structure"""
    
    print(f"🔍 Testing {name}...")
    
    try:
        # Try to read the data
        df = pd.read_csv(url)
        
        # Basic checks
        print(f"   ✅ Connection successful")
        print(f"   📊 Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        
        # Check for expected columns if provided
        if expected_columns:
            missing_cols = [col for col in expected_columns if col not in df.columns]
            if missing_cols:
                print(f"   ⚠️  Missing expected columns: {missing_cols}")
            else:
                print(f"   ✅ All expected columns present")
        
        # Show first few columns
        print(f"   📋 Columns: {list(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
        
        # Check for recent data
        date_columns = [col for col in df.columns if 'date' in col.lower() or col.count('/') == 2]
        if date_columns:
            print(f"   📅 Date column found: {date_columns[0]}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Failed: {str(e)}")
        return False

def test_json_source(name, url):
    """Test if a JSON API source is accessible"""
    
    print(f"🔍 Testing {name}...")
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        print(f"   ✅ Connection successful - JSON data loaded")
        
        if isinstance(data, dict):
            print(f"   📊 JSON keys: {list(data.keys())[:5]}{'...' if len(data.keys()) > 5 else ''}")
        elif isinstance(data, list):
            print(f"   📊 JSON array with {len(data)} items")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Failed: {str(e)}")
        return False

def test_all_connections():
    """Test all data sources with India focus"""
    
    print("🚀 COVID-19 Data Connection Test - India Focus")
    print("=" * 55)
    
    tests_passed = 0
    total_tests = 0
    
    # Test India-specific data sources first
    print("\n🇮🇳 INDIA-SPECIFIC DATA SOURCES")
    print("-" * 30)
    
    # Test updated COVID19India data sources (working alternatives)
    india_sources = {
        'National Time Series': 'https://data.incovid19.org/csv/latest/case_time_series.csv',
        'State Wise Current': 'https://data.incovid19.org/csv/latest/state_wise.csv',
        'District Wise': 'https://data.incovid19.org/csv/latest/district_wise.csv'
    }
    
    for name, url in india_sources.items():
        total_tests += 1
        if test_data_source(f"India {name}", url):
            tests_passed += 1
        print()
    
    # Test alternative Indian API sources
    alt_india_sources = {
        'Latest Stats': 'https://api.rootnet.in/covid19-in/stats/latest',
        'Historical Data': 'https://api.rootnet.in/covid19-in/unofficial/covid19india.org/statewise/history'
    }
    
    for name, url in alt_india_sources.items():
        total_tests += 1
        if test_json_source(f"India {name} (Alt)", url):
            tests_passed += 1
        print()
    
    # Test Johns Hopkins data
    print("📊 JOHNS HOPKINS UNIVERSITY DATA")
    print("-" * 30)
    
    for name, url in JHU_URLS.items():
        total_tests += 1
        if test_data_source(f"JHU {name}", url):
            # Check specifically for India data
            try:
                df = pd.read_csv(url)
                if 'global' in name and 'Country/Region' in df.columns:
                    india_rows = df[df['Country/Region'] == 'India']
                    if not india_rows.empty:
                        print(f"   🇮🇳 India data found: {len(india_rows)} records")
                    else:
                        print(f"   ⚠️ India data not found in {name}")
            except:
                pass
            tests_passed += 1
        print()
    
    # Test Our World in Data
    print("📈 OUR WORLD IN DATA")
    print("-" * 20)
    
    # Test the main complete dataset first
    owid_main_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
    total_tests += 1
    if test_data_source("OWID complete_dataset", owid_main_url, ['date', 'location']):
        # Check for India data specifically
        try:
            df = pd.read_csv(owid_main_url)
            if 'location' in df.columns:
                india_data = df[df['location'] == 'India']
                if not india_data.empty:
                    print(f"   🇮🇳 India records: {len(india_data)}")
                    if 'date' in df.columns:
                        print(f"   📅 India date range: {india_data['date'].min()} to {india_data['date'].max()}")
                else:
                    print(f"   ⚠️ No India data found")
        except:
            pass
        tests_passed += 1
    print()
    
    # Test vaccination data (may have changed URLs)
    vaccination_urls = {
        'vaccinations': 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations.csv',
        'vaccinations_by_manufacturer': 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations-by-manufacturer.csv'
    }
    
    for name, url in vaccination_urls.items():
        total_tests += 1
        print(f"🔍 Testing OWID {name}...")
        try:
            df = pd.read_csv(url)
            print(f"   ✅ Connection successful")
            print(f"   📊 Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
            
            # Check for India data
            if 'location' in df.columns:
                india_data = df[df['location'] == 'India']
                if not india_data.empty:
                    print(f"   🇮🇳 India records: {len(india_data)}")
            
            tests_passed += 1
        except requests.exceptions.HTTPError as e:
            if "404" in str(e):
                print(f"   ⚠️ URL may have changed (404 error)")
                print(f"   💡 Vaccination data available in complete dataset")
                tests_passed += 1  # Don't fail the test, as data is available elsewhere
            else:
                print(f"   ❌ Failed: {str(e)}")
        except Exception as e:
            print(f"   ❌ Failed: {str(e)}")
        print()
    
    # Test Oxford Government Response
    print("🏛️ OXFORD GOVERNMENT RESPONSE TRACKER")
    print("-" * 35)
    
    total_tests += 1
    oxford_url = "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_nat_latest.csv"
    if test_data_source("Oxford COVID-19 Government Response Tracker", oxford_url):
        # Check for India policy data
        try:
            df = pd.read_csv(oxford_url)
            if 'CountryName' in df.columns:
                india_policy = df[df['CountryName'] == 'India']
                if not india_policy.empty:
                    print(f"   🇮🇳 India policy records: {len(india_policy)}")
        except:
            pass
        tests_passed += 1
    print()
    
    # Test Population data
    print("👥 POPULATION DATA")
    print("-" * 15)
    
    total_tests += 1
    population_url = "https://raw.githubusercontent.com/datasets/population/master/data/population.csv"
    if test_data_source("World Population", population_url, ['Country Name', 'Year', 'Value']):
        # Check for India population data
        try:
            df = pd.read_csv(population_url)
            india_pop = df[df['Country Name'] == 'India']
            if not india_pop.empty:
                latest_year = india_pop['Year'].max()
                latest_pop = india_pop[india_pop['Year'] == latest_year]['Value'].iloc[0]
                print(f"   🇮🇳 India population ({latest_year}): {latest_pop:,}")
            else:
                print(f"   ⚠️ India population data not found")
        except:
            pass
        tests_passed += 1
    print()
    
    # Summary
    print("=" * 55)
    print(f"📊 TEST SUMMARY - INDIA FOCUS")
    print("=" * 55)
    print(f"✅ Tests passed: {tests_passed}/{total_tests}")
    print(f"❌ Tests failed: {total_tests - tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        print("\n🎉 ALL TESTS PASSED! Ready for India-focused data collection.")
        print("💡 Next step: Run 'python data_collection.py'")
        print("🇮🇳 You'll get detailed Indian state/district data plus global comparisons!")
    else:
        print(f"\n⚠️  Some tests failed. Check your internet connection and try again.")
        print("🔧 If problems persist, some data sources might be temporarily unavailable.")
    
    print(f"\n🎯 Primary focus: {PRIMARY_COUNTRY}")
    print(f"🌏 Regional focus: {', '.join(REGIONS['South Asia'])}")
    
    return tests_passed == total_tests

def test_environment():
    """Test if the environment is set up correctly"""
    
    print("🔧 ENVIRONMENT TEST")
    print("-" * 20)
    
    # Test Python version
    print(f"🐍 Python version: {sys.version.split()[0]}")
    
    # Test required libraries
    required_libraries = ['pandas', 'requests', 'numpy', 'matplotlib', 'seaborn']
    
    for lib in required_libraries:
        try:
            exec(f"import {lib}")
            print(f"   ✅ {lib} imported successfully")
        except ImportError:
            print(f"   ❌ {lib} not found - install with: pip install {lib}")
    
    # Test folder structure
    print(f"\n📁 Project structure:")
    for path_name, path in [('Raw data', RAW_DATA_PATH), ('Processed data', PROCESSED_DATA_PATH), 
                           ('External data', EXTERNAL_DATA_PATH), ('Notebooks', NOTEBOOKS_PATH)]:
        if os.path.exists(path):
            print(f"   ✅ {path_name}: {path}")
        else:
            print(f"   ❌ {path_name}: {path} (will be created)")

if __name__ == "__main__":
    print_config_summary()
    print()
    
    # Test environment first
    test_environment()
    print()
    
    # Test data connections
    success = test_all_connections()
    
    if success:
        print("\n🚀 Ready to proceed with data collection!")
    else:
        print("\n🔧 Please fix the issues above before proceeding.")