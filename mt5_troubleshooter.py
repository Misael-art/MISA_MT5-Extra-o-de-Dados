import os
import sys
import logging
import subprocess
import time
import ctypes
import configparser
import winreg
import platform

# Garantir que o diretório de logs existe
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/mt5_troubleshooter.log"),
        logging.StreamHandler()
    ]
)

def is_admin():
    """Check if the script is running with admin privileges"""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except:
        return False

def check_system_info():
    """Check system information"""
    logging.info("Checking system information...")
    
    # Get system info
    system = platform.system()
    release = platform.release()
    version = platform.version()
    architecture = platform.architecture()
    machine = platform.machine()
    processor = platform.processor()
    
    logging.info(f"System: {system}")
    logging.info(f"Release: {release}")
    logging.info(f"Version: {version}")
    logging.info(f"Architecture: {architecture}")
    logging.info(f"Machine: {machine}")
    logging.info(f"Processor: {processor}")
    
    # Check Python version
    python_version = platform.python_version()
    python_implementation = platform.python_implementation()
    
    logging.info(f"Python version: {python_version}")
    logging.info(f"Python implementation: {python_implementation}")
    
    # Check if running in virtual environment
    in_venv = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    logging.info(f"Running in virtual environment: {in_venv}")
    
    return {
        "system": system,
        "release": release,
        "version": version,
        "architecture": architecture,
        "machine": machine,
        "processor": processor,
        "python_version": python_version,
        "python_implementation": python_implementation,
        "in_venv": in_venv
    }

def check_mt5_installation():
    """Check MT5 installation"""
    logging.info("Checking MT5 installation...")
    
    # Common installation paths
    common_paths = [
        "C:\\Program Files\\MetaTrader 5",
        "C:\\Program Files (x86)\\MetaTrader 5",
        os.path.expanduser("~\\AppData\\Roaming\\MetaQuotes\\Terminal"),
        os.path.expanduser("~\\AppData\\Roaming\\MetaTrader 5")
    ]
    
    # Check config file first
    config_path = "config/config.ini"
    if os.path.exists(config_path):
        config = configparser.ConfigParser()
        config.read(config_path)
        if config.has_section('MT5') and config.has_option('MT5', 'path'):
            mt5_path = config.get('MT5', 'path')
            common_paths.insert(0, mt5_path)  # Add to the beginning of the list
    
    # Check registry
    try:
        registry_paths = [
            r"SOFTWARE\MetaQuotes\Terminal",
            r"SOFTWARE\WOW6432Node\MetaQuotes\Terminal"
        ]
        
        for reg_path in registry_paths:
            try:
                key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, reg_path)
                
                try:
                    i = 0
                    while True:
                        subkey_name = winreg.EnumKey(key, i)
                        subkey = winreg.OpenKey(key, subkey_name)
                        
                        try:
                            path_value = winreg.QueryValueEx(subkey, "Path")[0]
                            if path_value not in common_paths:
                                common_paths.append(path_value)
                        except:
                            pass
                        
                        winreg.CloseKey(subkey)
                        i += 1
                except WindowsError:
                    pass
                
                winreg.CloseKey(key)
            except:
                pass
    except:
        pass
    
    # Check each path
    found_paths = []
    for path in common_paths:
        if os.path.exists(path):
            terminal_exe = os.path.join(path, "terminal64.exe")
            if os.path.exists(terminal_exe):
                found_paths.append(path)
                logging.info(f"Found MT5 installation at: {path}")
    
    if not found_paths:
        logging.warning("No MT5 installation found")
        return None
    
    # Return the first found path
    return found_paths[0]

def check_mt5_running():
    """Check if MT5 is running"""
    try:
        result = subprocess.run(["tasklist", "/FI", "IMAGENAME eq terminal64.exe"], 
                               capture_output=True, text=True)
        running = "terminal64.exe" in result.stdout
        
        if running:
            logging.info("MT5 is currently running")
        else:
            logging.info("MT5 is not currently running")
        
        return running
    except Exception as e:
        logging.error(f"Error checking if MT5 is running: {e}")
        return False

def check_mt5_module():
    """Check MT5 Python module"""
    logging.info("Checking MT5 Python module...")
    
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

def test_mt5_connection(mt5_path):
    """Test MT5 connection"""
    logging.info("Testing MT5 connection...")
    
    try:
        import MetaTrader5 as mt5
        
        # Try to initialize MT5
        logging.info(f"Trying to initialize MT5 with path: {mt5_path}")
        if mt5.initialize(path=mt5_path):
            logging.info("MT5 initialized successfully")
            
            # Get MT5 version
            version_info = mt5.version()
            if version_info:
                logging.info(f"MT5 version: {version_info}")
            
            # Try to get symbols
            symbols = mt5.symbols_get()
            if symbols:
                logging.info(f"Retrieved {len(symbols)} symbols")
                return True
            else:
                logging.warning("No symbols retrieved")
        else:
            error_code = mt5.last_error()
            logging.error(f"Failed to initialize MT5: {error_code}")
            
            # Provide specific advice based on error code
            if "Authorization failed" in str(error_code):
                logging.info("SOLUTION: MT5 authorization failed. Make sure you are logged in to your MT5 account.")
            elif "IPC initialize failed" in str(error_code):
                logging.info("SOLUTION: IPC initialization failed. This could be due to:")
                logging.info("1. MT5 is not running or is running with different permissions")
                logging.info("2. The MT5 path is incorrect")
                logging.info("3. There are permission issues with the MT5 installation")
                logging.info("Try running both MT5 and this script as administrator")
            elif "IPC timeout" in str(error_code):
                logging.info("SOLUTION: IPC timeout. This could be due to:")
                logging.info("1. MT5 is still starting up")
                logging.info("2. MT5 is busy or unresponsive")
                logging.info("3. There are network issues")
                logging.info("Try restarting MT5 and waiting a few minutes before trying again")
        
        # Shutdown MT5
        mt5.shutdown()
    except Exception as e:
        logging.error(f"Error testing MT5 connection: {e}")
    
    return False

def check_firewall():
    """Check Windows Firewall settings"""
    logging.info("Checking Windows Firewall settings...")
    
    try:
        # Check if firewall is enabled
        result = subprocess.run(["netsh", "advfirewall", "show", "currentprofile"], 
                               capture_output=True, text=True)
        
        if "State                                 ON" in result.stdout:
            logging.info("Windows Firewall is enabled")
            
            # Check if MT5 is allowed
            result = subprocess.run(["netsh", "advfirewall", "firewall", "show", "rule", "name=all"], 
                                   capture_output=True, text=True)
            
            if "terminal64.exe" in result.stdout:
                logging.info("MT5 (terminal64.exe) has firewall rules")
            else:
                logging.warning("MT5 (terminal64.exe) does not have explicit firewall rules")
                logging.info("SOLUTION: Add firewall rules for MT5 (terminal64.exe)")
        else:
            logging.info("Windows Firewall is disabled")
    except Exception as e:
        logging.error(f"Error checking firewall settings: {e}")

def check_antivirus():
    """Check for common antivirus software"""
    logging.info("Checking for antivirus software...")
    
    # Common antivirus processes
    av_processes = [
        "avp.exe",          # Kaspersky
        "mcshield.exe",     # McAfee
        "msmpeng.exe",      # Windows Defender
        "avastui.exe",      # Avast
        "avgui.exe",        # AVG
        "bdagent.exe",      # Bitdefender
        "ekrn.exe",         # ESET
        "fsav.exe",         # F-Secure
        "navapsvc.exe",     # Norton
        "pccntmon.exe"      # Trend Micro
    ]
    
    found_av = []
    
    try:
        result = subprocess.run(["tasklist"], capture_output=True, text=True)
        
        for av in av_processes:
            if av in result.stdout:
                found_av.append(av)
                logging.info(f"Found antivirus process: {av}")
        
        if "msmpeng.exe" in result.stdout:
            logging.info("Windows Defender is running")
            logging.info("SOLUTION: Add MT5 to Windows Defender exclusions")
        
        if found_av:
            logging.info("SOLUTION: Add MT5 to antivirus exclusions")
        else:
            logging.info("No common antivirus processes found")
    except Exception as e:
        logging.error(f"Error checking antivirus software: {e}")

def check_permissions(mt5_path):
    """Check file permissions"""
    logging.info("Checking file permissions...")
    
    if not mt5_path:
        logging.warning("MT5 path not provided, skipping permission check")
        return
    
    try:
        # Check if we can access the MT5 directory
        if os.access(mt5_path, os.R_OK):
            logging.info(f"Have read access to MT5 directory: {mt5_path}")
        else:
            logging.warning(f"Do not have read access to MT5 directory: {mt5_path}")
            logging.info("SOLUTION: Run as administrator or fix permissions")
        
        # Check if we can access terminal64.exe
        terminal_exe = os.path.join(mt5_path, "terminal64.exe")
        if os.access(terminal_exe, os.R_OK | os.X_OK):
            logging.info(f"Have read and execute access to terminal64.exe")
        else:
            logging.warning(f"Do not have read and execute access to terminal64.exe")
            logging.info("SOLUTION: Run as administrator or fix permissions")
        
        # Check if we can write to the MT5 directory
        if os.access(mt5_path, os.W_OK):
            logging.info(f"Have write access to MT5 directory: {mt5_path}")
        else:
            logging.warning(f"Do not have write access to MT5 directory: {mt5_path}")
            logging.info("SOLUTION: Run as administrator or fix permissions")
    except Exception as e:
        logging.error(f"Error checking permissions: {e}")

def check_network():
    """Check network connectivity"""
    logging.info("Checking network connectivity...")
    
    try:
        # Check if we can ping google.com
        result = subprocess.run(["ping", "-n", "1", "google.com"], 
                               capture_output=True, text=True)
        
        if "Reply from" in result.stdout:
            logging.info("Network connectivity: OK")
        else:
            logging.warning("Network connectivity: Failed")
            logging.info("SOLUTION: Check your internet connection")
    except Exception as e:
        logging.error(f"Error checking network connectivity: {e}")

def suggest_solutions():
    """Suggest solutions based on common issues"""
    logging.info("\n=== SUGGESTED SOLUTIONS ===")
    
    logging.info("1. Run both MT5 and this script as administrator")
    logging.info("2. Make sure you are logged in to your MT5 account")
    logging.info("3. Add MT5 to antivirus and firewall exclusions")
    logging.info("4. Reinstall the MT5 Python module: pip install --upgrade MetaTrader5")
    logging.info("5. Try using a different MT5 account or demo account")
    logging.info("6. Check if your MT5 broker allows API connections")
    logging.info("7. Try using a different approach, such as the MT5 WebAPI or a different trading platform")
    logging.info("8. Contact your broker's support for assistance")

def main():
    """Main function"""
    logging.info("Starting MT5 troubleshooter")
    
    # Check if running as admin
    admin = is_admin()
    logging.info(f"Running as administrator: {admin}")
    
    if not admin:
        logging.warning("Not running as administrator. Some checks may fail.")
        logging.info("SOLUTION: Run this script as administrator")
    
    # Check system info
    check_system_info()
    
    # Check MT5 installation
    mt5_path = check_mt5_installation()
    
    # Check if MT5 is running
    mt5_running = check_mt5_running()
    
    # Check MT5 module
    check_mt5_module()
    
    # Check permissions
    check_permissions(mt5_path)
    
    # Check firewall
    check_firewall()
    
    # Check antivirus
    check_antivirus()
    
    # Check network
    check_network()
    
    # Test MT5 connection
    if mt5_path and mt5_running:
        test_mt5_connection(mt5_path)
    
    # Suggest solutions
    suggest_solutions()
    
    logging.info("MT5 troubleshooter completed")
    
    print("\nTroubleshooting completed. Check mt5_troubleshooter.log for details and solutions.")

if __name__ == "__main__":
    main()
