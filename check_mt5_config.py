import os
import sys
import configparser
import logging
import ctypes
import winreg
import subprocess
import platform
import psutil
import re

# Garantir que o diretório de logs existe
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/mt5_config_check.log"),
        logging.StreamHandler()
    ]
)

def is_admin():
    """Check if the script is running with admin privileges"""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except:
        return False

def check_mt5_registry():
    """Check MT5 registry entries"""
    logging.info("Checking MT5 registry entries...")
    
    registry_paths = [
        r"SOFTWARE\MetaQuotes\Terminal",
        r"SOFTWARE\WOW6432Node\MetaQuotes\Terminal"
    ]
    
    found = False
    
    for reg_path in registry_paths:
        try:
            key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, reg_path)
            logging.info(f"Found registry key: {reg_path}")
            
            try:
                i = 0
                while True:
                    subkey_name = winreg.EnumKey(key, i)
                    subkey = winreg.OpenKey(key, subkey_name)
                    
                    try:
                        path_value = winreg.QueryValueEx(subkey, "Path")[0]
                        logging.info(f"MT5 path from registry: {path_value}")
                        found = True
                    except:
                        pass
                    
                    winreg.CloseKey(subkey)
                    i += 1
            except WindowsError:
                pass
            
            winreg.CloseKey(key)
        except:
            logging.info(f"Registry key not found: {reg_path}")
    
    if not found:
        logging.warning("No MT5 registry entries found")
    
    return found

def check_mt5_installation(mt5_path):
    """Check MT5 installation files and permissions"""
    logging.info(f"Checking MT5 installation at: {mt5_path}")
    
    if not os.path.exists(mt5_path):
        logging.error(f"MT5 path does not exist: {mt5_path}")
        return False
    
    # Check terminal64.exe
    terminal_exe = os.path.join(mt5_path, "terminal64.exe")
    if not os.path.exists(terminal_exe):
        logging.error(f"terminal64.exe not found at: {terminal_exe}")
        return False
    
    logging.info(f"terminal64.exe found at: {terminal_exe}")
    
    # Check if we can access the file
    try:
        with open(terminal_exe, "rb") as f:
            # Just read a few bytes to check access
            f.read(10)
        logging.info("terminal64.exe is accessible")
    except Exception as e:
        logging.error(f"Cannot access terminal64.exe: {e}")
        return False
    
    # Check MQL5 folder
    mql5_path = os.path.join(mt5_path, "MQL5")
    if not os.path.exists(mql5_path):
        logging.warning(f"MQL5 folder not found at: {mql5_path}")
    else:
        logging.info(f"MQL5 folder found at: {mql5_path}")
    
    # Check if MT5 is running
    try:
        result = subprocess.run(["tasklist", "/FI", "IMAGENAME eq terminal64.exe"], 
                               capture_output=True, text=True)
        if "terminal64.exe" in result.stdout:
            logging.info("MT5 is currently running")
        else:
            logging.warning("MT5 is not currently running")
    except Exception as e:
        logging.error(f"Error checking if MT5 is running: {e}")
    
    return True

def check_python_mt5_module():
    """Check Python MT5 module installation"""
    logging.info("Checking Python MT5 module...")
    
    try:
        import MetaTrader5 as mt5
        logging.info(f"MT5 module found: {mt5.__file__}")
        
        # Check version
        try:
            logging.info(f"MT5 module version: {mt5.__version__}")
        except:
            logging.warning("Could not determine MT5 module version")
        
        return True
    except ImportError as e:
        logging.error(f"MT5 module not found: {e}")
        return False
    except Exception as e:
        logging.error(f"Error importing MT5 module: {e}")
        return False

def check_mt5_config_file():
    """Check MT5 config file"""
    logging.info("Checking MT5 config file...")
    
    config_path = "config/config.ini"
    if not os.path.exists(config_path):
        logging.error(f"Config file not found: {config_path}")
        return None
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    if not config.has_section('MT5'):
        logging.error("Config file does not have MT5 section")
        return None
    
    if not config.has_option('MT5', 'path'):
        logging.error("Config file does not have MT5 path option")
        return None
    
    mt5_path = config.get('MT5', 'path')
    logging.info(f"MT5 path from config: {mt5_path}")
    
    if not os.path.exists(mt5_path):
        logging.error(f"MT5 path from config does not exist: {mt5_path}")
        return None
    
    return mt5_path

def check_mt5_logs():
    """Check MT5 logs for errors"""
    logging.info("Checking MT5 logs...")
    
    # Get MT5 path from config
    config_path = "config/config.ini"
    if not os.path.exists(config_path):
        logging.error(f"Config file not found: {config_path}")
        return False
    
    config = configparser.ConfigParser()
    config.read(config_path)
    mt5_path = config.get('MT5', 'path', fallback=None)
    
    if not mt5_path or not os.path.exists(mt5_path):
        logging.error(f"Invalid MT5 path: {mt5_path}")
        return False
    
    # Check logs folder
    logs_path = os.path.join(mt5_path, "logs")
    if not os.path.exists(logs_path):
        logging.warning(f"MT5 logs folder not found: {logs_path}")
        return False
    
    logging.info(f"MT5 logs folder found: {logs_path}")
    
    # List log files
    log_files = [f for f in os.listdir(logs_path) if f.endswith(".log")]
    logging.info(f"Found {len(log_files)} log files")
    
    # Check most recent log file
    if log_files:
        log_files.sort(key=lambda x: os.path.getmtime(os.path.join(logs_path, x)), reverse=True)
        most_recent = log_files[0]
        logging.info(f"Most recent log file: {most_recent}")
        
        # Check for errors in the log file
        log_file_path = os.path.join(logs_path, most_recent)
        try:
            with open(log_file_path, "r", encoding="utf-8", errors="ignore") as f:
                log_content = f.read()
                
                # Check for common error messages
                error_keywords = ["error", "failed", "cannot", "denied", "permission"]
                for keyword in error_keywords:
                    if keyword.lower() in log_content.lower():
                        logging.warning(f"Found '{keyword}' in log file")
                        
                        # Extract lines with errors
                        lines = log_content.split("\n")
                        error_lines = [line for line in lines if keyword.lower() in line.lower()]
                        for line in error_lines[:10]:  # Show first 10 error lines
                            logging.warning(f"Log error: {line.strip()}")
        except Exception as e:
            logging.error(f"Error reading log file: {e}")
    
    return True

def main():
    """Main function to check MT5 configuration"""
    logging.info("Starting MT5 configuration check")
    
    # Check if running as admin
    admin = is_admin()
    logging.info(f"Running as administrator: {admin}")
    
    if not admin:
        logging.warning("Not running as administrator. Some checks may fail.")
    
    # Check MT5 config file
    mt5_path = check_mt5_config_file()
    
    # Check MT5 registry
    check_mt5_registry()
    
    # Check MT5 installation
    if mt5_path:
        check_mt5_installation(mt5_path)
    
    # Check Python MT5 module
    check_python_mt5_module()
    
    # Check MT5 logs
    check_mt5_logs()
    
    logging.info("MT5 configuration check completed")

if __name__ == "__main__":
    main()
