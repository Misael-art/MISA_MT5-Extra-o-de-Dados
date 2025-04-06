import os
import sys
import logging
import time
import subprocess
import json
import pandas as pd
import datetime
import sqlite3
import configparser

# Garantir que o diretório de logs existe
os.makedirs("logs", exist_ok=True)

# Procurar por configurações de logging e modificá-las
# Se houver algo como:
# logging.basicConfig(...
#     handlers=[
#         logging.FileHandler("mt5_workaround.log", ...),
# Modificar para:
# logging.basicConfig(...
#     handlers=[
#         logging.FileHandler("logs/mt5_workaround.log", ...),

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/mt5_workaround.log"),
        logging.StreamHandler()
    ]
)

class MT5Workaround:
    def __init__(self):
        """Initialize the MT5 workaround"""
        logging.info("Initializing MT5 workaround")
        
        # Load configuration
        self.load_config()
        
        # Initialize database
        self.initialize_database()
        
        # Define predefined symbols
        self.symbols = [
            # Futuros do Mercado Brasileiro
            "WIN$N", "DOL$N", "WDO$N", "IND$N", "SOJ$N", "MIL$N", "BOV$N", "CAF$N", "BGI$N",
            # Bitcoin e criptomoedas
            "BTCUSD",
            # Pares de moedas principais
            "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
            # Pares cruzados
            "EURGBP", "EURJPY", "GBPJPY",
            # Exóticos
            "USDBRL", "EURTRY",
            # ETFs populares
            "SPY", "QQQ", "IWM", "EEM", "VTI", "GLD", "SLV", "XLF", "XLE"
        ]
    
    def load_config(self):
        """Load configuration from config.ini"""
        logging.info("Loading configuration")
        
        self.config = configparser.ConfigParser()
        
        config_path = "config/config.ini"
        if not os.path.exists(config_path):
            logging.error(f"Config file not found: {config_path}")
            sys.exit(1)
        
        self.config.read(config_path)
        
        # Get MT5 path
        self.mt5_path = self.config.get('MT5', 'path', fallback=None)
        if not self.mt5_path or not os.path.exists(self.mt5_path):
            logging.error(f"Invalid MT5 path: {self.mt5_path}")
            sys.exit(1)
        
        # Get database path
        self.db_type = self.config.get('DATABASE', 'type', fallback='sqlite')
        self.db_path = self.config.get('DATABASE', 'path', fallback='database/mt5_data.db')
        
        logging.info(f"MT5 path: {self.mt5_path}")
        logging.info(f"Database type: {self.db_type}")
        logging.info(f"Database path: {self.db_path}")
    
    def initialize_database(self):
        """Initialize the database"""
        logging.info("Initializing database")
        
        if self.db_type != 'sqlite':
            logging.error(f"Unsupported database type: {self.db_type}")
            sys.exit(1)
        
        # Create database directory if it doesn't exist
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Connect to database
        try:
            self.conn = sqlite3.connect(self.db_path)
            logging.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            sys.exit(1)
    
    def create_tables(self):
        """Create database tables for each symbol"""
        logging.info("Creating database tables")
        
        cursor = self.conn.cursor()
        
        for symbol in self.symbols:
            table_name = f"{symbol.lower()}_1min"
            
            try:
                # Create table if it doesn't exist
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    time TIMESTAMP PRIMARY KEY,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    tick_volume INTEGER,
                    spread INTEGER,
                    real_volume INTEGER,
                    rsi REAL,
                    macd_line REAL,
                    macd_signal REAL,
                    macd_histogram REAL,
                    ma_20 REAL,
                    bb_upper REAL,
                    bb_middle REAL,
                    bb_lower REAL,
                    atr REAL,
                    candle_pattern TEXT,
                    trader_sentiment REAL,
                    last REAL,
                    trading_hours INTEGER
                )
                """)
                
                logging.info(f"Created table: {table_name}")
            except Exception as e:
                logging.error(f"Error creating table {table_name}: {e}")
        
        self.conn.commit()
    
    def generate_sample_data(self):
        """Generate sample data for each symbol"""
        logging.info("Generating sample data")
        
        # Current time
        now = datetime.datetime.now()
        
        # Generate data for the last 60 minutes
        for symbol in self.symbols:
            table_name = f"{symbol.lower()}_1min"
            
            # Generate random data
            data = []
            base_price = 100.0  # Base price for the symbol
            
            if "USD" in symbol:
                base_price = 1.0
            elif "EUR" in symbol:
                base_price = 1.2
            elif "GBP" in symbol:
                base_price = 1.3
            elif "JPY" in symbol:
                base_price = 110.0
            elif "BTC" in symbol:
                base_price = 50000.0
            elif "WIN" in symbol:
                base_price = 120000.0
            elif "DOL" in symbol:
                base_price = 5000.0
            
            # Generate data for each minute
            for i in range(60):
                minute = now - datetime.timedelta(minutes=i)
                
                # Random price movement
                price_change = (0.5 - 0.5 * (i / 60)) * base_price * 0.01  # Slight upward trend
                
                # Calculate prices
                close_price = base_price + price_change
                open_price = close_price - (0.5 - 0.5 * (i / 60)) * base_price * 0.005
                high_price = max(close_price, open_price) + base_price * 0.002
                low_price = min(close_price, open_price) - base_price * 0.002
                
                # Calculate indicators
                rsi = 50 + (0.5 - 0.5 * (i / 60)) * 20  # RSI between 30 and 70
                macd_line = (0.5 - 0.5 * (i / 60)) * 0.5
                macd_signal = (0.5 - 0.5 * (i / 60)) * 0.3
                macd_histogram = macd_line - macd_signal
                ma_20 = base_price
                bb_middle = base_price
                bb_upper = base_price + base_price * 0.01
                bb_lower = base_price - base_price * 0.01
                atr = base_price * 0.005
                
                # Other fields
                candle_pattern = "bullish" if close_price > open_price else "bearish"
                trader_sentiment = 0.5 + (0.5 - 0.5 * (i / 60)) * 0.2  # Between 0.3 and 0.7
                
                data.append({
                    "time": minute.strftime("%Y-%m-%d %H:%M:%S"),
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "tick_volume": int(1000 + 500 * (0.5 - 0.5 * (i / 60))),
                    "spread": int(2 + 3 * (0.5 - 0.5 * (i / 60))),
                    "real_volume": int(10000 + 5000 * (0.5 - 0.5 * (i / 60))),
                    "rsi": rsi,
                    "macd_line": macd_line,
                    "macd_signal": macd_signal,
                    "macd_histogram": macd_histogram,
                    "ma_20": ma_20,
                    "bb_upper": bb_upper,
                    "bb_middle": bb_middle,
                    "bb_lower": bb_lower,
                    "atr": atr,
                    "candle_pattern": candle_pattern,
                    "trader_sentiment": trader_sentiment,
                    "last": close_price,
                    "trading_hours": 1
                })
            
            # Insert data into database
            try:
                df = pd.DataFrame(data)
                df.to_sql(table_name, self.conn, if_exists='replace', index=False)
                logging.info(f"Generated sample data for {symbol}")
            except Exception as e:
                logging.error(f"Error generating sample data for {symbol}: {e}")
    
    def run(self):
        """Run the workaround"""
        logging.info("Running MT5 workaround")
        
        # Create tables
        self.create_tables()
        
        # Generate sample data
        self.generate_sample_data()
        
        logging.info("MT5 workaround completed successfully")
        
        print("\nMT5 workaround completed successfully.")
        print("Sample data has been generated for all symbols.")
        print("You can now run the application with simulated data.")
        print("Note: This is a temporary workaround until the MT5 connection issues are resolved.")

def main():
    """Main function"""
    logging.info("Starting MT5 workaround")
    
    workaround = MT5Workaround()
    workaround.run()
    
    logging.info("MT5 workaround completed")

if __name__ == "__main__":
    main()
