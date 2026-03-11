import logging
import sys
import os

# Set up logging to stdout
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

try:
    from database.db import init_schema
    print("Testing database connection and schema initialization...")
    init_schema()
    print("SUCCESS: Database connection verified.")
except Exception as e:
    print(f"FAILED: Database connection error: {e}")
    sys.exit(1)
