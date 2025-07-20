import pandas as pd
import requests
import numpy as np
from datetime import datetime, timedelta
import os
import logging
from typing import Optional, Dict, Any
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('covid_data_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CovidDataScraper:
    """
    A comprehensive COVID-19 data scraper for Our World in Data dataset
    """
    
    def __init__(self, config_path: str = 'config/settings.yaml'):
        """Initialize the scraper with configuration"""
        self.config = self._load_config(config_path)
        self.base_url = "https://covid.ourworldindata.org/data"
        self.data_dir = self.config.get('data_dir', 'data')
        self._ensure_directories()
        
    def _load_config(self, config_path: str) -> Dict[Any, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file) or {}
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return {
                'data_dir': 'data',
                'max_retries': 3,
                'timeout': 300,
                'chunk_size': 10000
            }
    
    def _ensure_directories(self):
        """Create necessary directories if they don't exist"""
        directories = [
            f"{self.data_dir}/raw",
            f"{self.data_dir}/processed", 
            f"{self.data_dir}/exports",
            "logs"
        ]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def scrape_main_dataset(self, save_raw: bool = True) -> pd.DataFrame:
        """
        Scrape the main COVID-19 dataset
        
        Args:
            save_raw: Whether to save the raw data to file
            
        Returns:
            DataFrame containing the COVID-19 data
        """
        url = f"{self.base_url}/owid-covid-data.csv"
        logger.info(f"Starting to scrape main dataset from {url}")
        
        try:
            # Download with progress tracking for large file
            response = requests.get(url, timeout=self.config.get('timeout', 300))
            response.raise_for_status()
            
            # Parse CSV
            df = pd.read_csv(url, low_memory=False)
            logger.info(f"Successfully scraped {len(df)} rows and {len(df.columns)} columns")
            
            # Data validation
            self._validate_main_dataset(df)
            
            if save_raw:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                raw_file = f"{self.data_dir}/raw/owid_covid_data_{timestamp}.csv"
                df.to_csv(raw_file, index=False)
                logger.info(f"Raw data saved to {raw_file}")
                
                # Also save as latest
                latest_file = f"{self.data_dir}/raw/owid_covid_data_latest.csv"
                df.to_csv(latest_file, index=False)
                logger.info(f"Latest data saved to {latest_file}")
            
            return df
            
        except requests.RequestException as e:
            logger.error(f"Error downloading data: {e}")
            raise
        except pd.errors.ParserError as e:
            logger.error(f"Error parsing CSV: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    
    def scrape_latest_data(self, save_raw: bool = True) -> pd.DataFrame:
        """
        Scrape the latest snapshot (current values only)
        
        Args:
            save_raw: Whether to save the raw data to file
            
        Returns:
            DataFrame containing latest COVID-19 data
        """
        url = f"{self.base_url}/latest/owid-covid-latest.csv"
        logger.info(f"Scraping latest data from {url}")
        
        try:
            df = pd.read_csv(url, low_memory=False)
            logger.info(f"Successfully scraped latest data: {len(df)} countries")
            
            if save_raw:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                latest_file = f"{self.data_dir}/raw/owid_covid_latest_{timestamp}.csv"
                df.to_csv(latest_file, index=False)
                logger.info(f"Latest snapshot saved to {latest_file}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error scraping latest data: {e}")
            raise
    
    def _validate_main_dataset(self, df: pd.DataFrame):
        """Validate the scraped dataset"""
        logger.info("Validating dataset...")
        
        # Check required columns
        required_cols = [
            'iso_code', 'continent', 'location', 'date',
            'total_cases', 'new_cases', 'total_deaths', 'new_deaths'
        ]
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Check data types
        if 'date' in df.columns:
            try:
                pd.to_datetime(df['date'])
            except:
                logger.warning("Date column may have formatting issues")
        
        # Check for completely empty dataset
        if df.empty:
            raise ValueError("Dataset is empty")
        
        # Log dataset statistics
        logger.info(f"Dataset validation passed:")
        logger.info(f"  - Rows: {len(df):,}")
        logger.info(f"  - Columns: {len(df.columns)}")
        logger.info(f"  - Date range: {df['date'].min()} to {df['date'].max()}")
        logger.info(f"  - Countries: {df['location'].nunique()}")
        logger.info(f"  - Missing values: {df.isnull().sum().sum():,}")
    
    def get_data_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate a summary of the dataset"""
        summary = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'date_range': {
                'start': df['date'].min() if 'date' in df.columns else None,
                'end': df['date'].max() if 'date' in df.columns else None
            },
            'countries_count': df['location'].nunique() if 'location' in df.columns else None,
            'continents': df['continent'].unique().tolist() if 'continent' in df.columns else None,
            'last_updated': datetime.now().isoformat(),
            'missing_values': df.isnull().sum().to_dict(),
            'key_metrics': {}
        }
        
        # Calculate key global metrics if data exists
        if 'total_cases' in df.columns:
            latest_data = df.groupby('location')['total_cases'].last()
            summary['key_metrics']['global_cases'] = int(latest_data.sum())
            
        if 'total_deaths' in df.columns:
            latest_data = df.groupby('location')['total_deaths'].last()
            summary['key_metrics']['global_deaths'] = int(latest_data.sum())
        
        return summary
    
    def save_metadata(self, df: pd.DataFrame):
        """Save dataset metadata and summary"""
        summary = self.get_data_summary(df)
        
        # Save as JSON
        import json
        metadata_file = f"{self.data_dir}/processed/dataset_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"Metadata saved to {metadata_file}")
        
        # Save column info
        column_info = {
            'columns': df.columns.tolist(),
            'dtypes': df.dtypes.astype(str).to_dict(),
            'non_null_counts': df.count().to_dict()
        }
        
        column_file = f"{self.data_dir}/processed/column_info.json"
        with open(column_file, 'w') as f:
            json.dump(column_info, f, indent=2)
        logger.info(f"Column information saved to {column_file}")

def main():
    """Main execution function"""
    logger.info("=== COVID-19 Data Scraper Started ===")
    
    try:
        # Initialize scraper
        scraper = CovidDataScraper()
        
        # Scrape main dataset
        logger.info("Scraping main time-series dataset...")
        main_df = scraper.scrape_main_dataset(save_raw=True)
        
        # Scrape latest snapshot
        logger.info("Scraping latest snapshot...")
        latest_df = scraper.scrape_latest_data(save_raw=True)
        
        # Generate and save metadata
        scraper.save_metadata(main_df)
        
        # Print summary
        summary = scraper.get_data_summary(main_df)
        logger.info("=== Scraping Summary ===")
        logger.info(f"Total records: {summary['total_rows']:,}")
        logger.info(f"Countries: {summary['countries_count']}")
        logger.info(f"Date range: {summary['date_range']['start']} to {summary['date_range']['end']}")
        
        if summary['key_metrics']:
            logger.info(f"Global cases: {summary['key_metrics'].get('global_cases', 'N/A'):,}")
            logger.info(f"Global deaths: {summary['key_metrics'].get('global_deaths', 'N/A'):,}")
        
        logger.info("=== COVID-19 Data Scraper Completed Successfully ===")
        
        return main_df, latest_df
        
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    df_main, df_latest = main()
    
    # Quick data exploration
    print("\n=== Quick Data Preview ===")
    print(f"Main dataset shape: {df_main.shape}")
    print(f"Latest dataset shape: {df_latest.shape}")
    
    print("\nColumn names:")
    print(df_main.columns.tolist())
    
    print("\nFirst few rows:")
    print(df_main.head())
    
    print("\nData types:")
    print(df_main.dtypes)
    
    print("\nMissing values summary:")
    print(df_main.isnull().sum().head(10))