import os
import sys
import configparser
import logging
import MetaTrader5 as mt5
import time
import subprocess

# Garantir que o diretório de logs existe
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/mt5_test.log"),
        logging.StreamHandler()
    ]
)

def main():
    """Test MT5 connection and symbol retrieval"""
    logging.info("Starting MT5 connection test")
    
    # Load MT5 path from config
    config = configparser.ConfigParser()
    config_path = "config/config.ini"
    
    if not os.path.exists(config_path):
        logging.error(f"Config file not found: {config_path}")
        return False
    
    config.read(config_path)
    mt5_path = config.get('MT5', 'path', fallback=None)
    
    if not mt5_path or not os.path.exists(mt5_path):
        logging.error(f"Invalid MT5 path: {mt5_path}")
        return False
    
    logging.info(f"MT5 path: {mt5_path}")
    
    # Try to initialize MT5
    logging.info("Initializing MT5...")
    if not mt5.initialize(path=mt5_path):
        error_code = mt5.last_error()
        logging.error(f"Failed to initialize MT5: {error_code}")
        
        # Try running as administrator
        logging.info("MT5 initialization failed. You may need to run this script as administrator.")
        return False
    
    logging.info("MT5 initialized successfully")
    
    # Get MT5 version
    version_info = mt5.version()
    if version_info:
        logging.info(f"MT5 version: {version_info}")
    else:
        logging.warning("Could not get MT5 version")
    
    # Try to get symbols
    logging.info("Getting symbols...")
    try:
        symbols = mt5.symbols_get()
        if symbols:
            logging.info(f"Retrieved {len(symbols)} symbols")
            # Print first 5 symbols
            for i, symbol in enumerate(symbols[:5]):
                logging.info(f"Symbol {i+1}: {symbol.name}")
        else:
            logging.warning("No symbols retrieved")
            
        # Try alternative method
        logging.info("Trying symbols_get(group='*')...")
        symbols_alt = mt5.symbols_get(group="*")
        if symbols_alt:
            logging.info(f"Retrieved {len(symbols_alt)} symbols with group='*'")
        else:
            logging.warning("No symbols retrieved with group='*'")
    except Exception as e:
        logging.error(f"Error retrieving symbols: {e}")
    
    # Try to get specific symbol info
    test_symbols = ["EURUSD", "USDJPY", "GBPUSD", "AUDUSD", "USDCHF"]
    logging.info("Testing specific symbols...")
    for symbol in test_symbols:
        try:
            symbol_info = mt5.symbol_info(symbol)
            if symbol_info:
                logging.info(f"Symbol {symbol} info: available={symbol_info.visible}, trade_mode={symbol_info.trade_mode}")
            else:
                logging.warning(f"Symbol {symbol} not found")
        except Exception as e:
            logging.error(f"Error getting info for {symbol}: {e}")
    
    # Check account info
    logging.info("Getting account info...")
    try:
        account_info = mt5.account_info()
        if account_info:
            logging.info(f"Account: {account_info.login}, Server: {account_info.server}")
        else:
            logging.warning("Could not get account info")
    except Exception as e:
        logging.error(f"Error getting account info: {e}")
    
    # Shutdown MT5
    mt5.shutdown()
    logging.info("MT5 test completed")
    return True

if __name__ == "__main__":
    main()
