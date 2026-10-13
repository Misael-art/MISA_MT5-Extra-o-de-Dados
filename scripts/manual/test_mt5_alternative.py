import os
import sys
import logging
import time
import subprocess

# Garantir que o diretório de logs existe
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/mt5_alt_test.log"),
        logging.StreamHandler()
    ]
)

def check_mt5_running():
    """Check if MT5 is running"""
    try:
        result = subprocess.run(["tasklist", "/FI", "IMAGENAME eq terminal64.exe"], 
                               capture_output=True, text=True)
        return "terminal64.exe" in result.stdout
    except Exception as e:
        logging.error(f"Error checking if MT5 is running: {e}")
        return False

def restart_mt5(mt5_path):
    """Restart MT5"""
    logging.info("Attempting to restart MT5...")
    
    # First, kill any running MT5 instances
    try:
        subprocess.run(["taskkill", "/F", "/IM", "terminal64.exe"], 
                      capture_output=True, text=True)
        logging.info("Killed existing MT5 processes")
    except Exception as e:
        logging.error(f"Error killing MT5 processes: {e}")
    
    # Wait a moment
    time.sleep(2)
    
    # Start MT5
    terminal_exe = os.path.join(mt5_path, "terminal64.exe")
    try:
        subprocess.Popen([terminal_exe])
        logging.info(f"Started MT5 from: {terminal_exe}")
        
        # Wait for MT5 to start
        time.sleep(5)
        
        return check_mt5_running()
    except Exception as e:
        logging.error(f"Error starting MT5: {e}")
        return False

def test_mt5_connection():
    """Test MT5 connection using alternative methods"""
    logging.info("Testing MT5 connection using alternative methods...")
    
    # Check if MT5 is running
    if not check_mt5_running():
        logging.warning("MT5 is not running")
        return False
    
    # Try to import MT5 module
    try:
        import MetaTrader5 as mt5
        logging.info("MT5 module imported successfully")
    except Exception as e:
        logging.error(f"Error importing MT5 module: {e}")
        return False
    
    # Try to initialize MT5 without specifying path
    logging.info("Trying to initialize MT5 without path...")
    try:
        if mt5.initialize():
            logging.info("MT5 initialized successfully without path")
            
            # Get MT5 version
            version_info = mt5.version()
            if version_info:
                logging.info(f"MT5 version: {version_info}")
            
            # Try to get symbols
            symbols = mt5.symbols_get()
            if symbols:
                logging.info(f"Retrieved {len(symbols)} symbols")
            else:
                logging.warning("No symbols retrieved")
            
            # Shutdown MT5
            mt5.shutdown()
            logging.info("MT5 shutdown successfully")
            return True
        else:
            error_code = mt5.last_error()
            logging.error(f"Failed to initialize MT5 without path: {error_code}")
    except Exception as e:
        logging.error(f"Error initializing MT5 without path: {e}")
    
    # Try with explicit path from environment variable
    mt5_path = os.environ.get("MT5_PATH", "C:\\Program Files\\MetaTrader 5")
    logging.info(f"Trying to initialize MT5 with path from environment: {mt5_path}")
    
    try:
        if mt5.initialize(path=mt5_path):
            logging.info("MT5 initialized successfully with path from environment")
            
            # Get MT5 version
            version_info = mt5.version()
            if version_info:
                logging.info(f"MT5 version: {version_info}")
            
            # Try to get symbols
            symbols = mt5.symbols_get()
            if symbols:
                logging.info(f"Retrieved {len(symbols)} symbols")
            else:
                logging.warning("No symbols retrieved")
            
            # Shutdown MT5
            mt5.shutdown()
            logging.info("MT5 shutdown successfully")
            return True
        else:
            error_code = mt5.last_error()
            logging.error(f"Failed to initialize MT5 with path from environment: {error_code}")
    except Exception as e:
        logging.error(f"Error initializing MT5 with path from environment: {e}")
    
    # Try restarting MT5
    if restart_mt5(mt5_path):
        logging.info("MT5 restarted successfully, trying to initialize again...")
        
        # Wait a moment for MT5 to fully start
        time.sleep(10)
        
        try:
            if mt5.initialize():
                logging.info("MT5 initialized successfully after restart")
                
                # Get MT5 version
                version_info = mt5.version()
                if version_info:
                    logging.info(f"MT5 version: {version_info}")
                
                # Try to get symbols
                symbols = mt5.symbols_get()
                if symbols:
                    logging.info(f"Retrieved {len(symbols)} symbols")
                else:
                    logging.warning("No symbols retrieved")
                
                # Shutdown MT5
                mt5.shutdown()
                logging.info("MT5 shutdown successfully")
                return True
            else:
                error_code = mt5.last_error()
                logging.error(f"Failed to initialize MT5 after restart: {error_code}")
        except Exception as e:
            logging.error(f"Error initializing MT5 after restart: {e}")
    
    logging.warning("All MT5 connection attempts failed")
    return False

def main():
    """Main function"""
    logging.info("Starting alternative MT5 connection test")
    
    result = test_mt5_connection()
    
    if result:
        logging.info("MT5 connection test successful")
    else:
        logging.error("MT5 connection test failed")
    
    logging.info("Alternative MT5 connection test completed")

if __name__ == "__main__":
    main()
