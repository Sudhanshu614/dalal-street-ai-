"""
Streamlit Cloud Entry Point
This file exists to ensure Streamlit Cloud can find and run the app correctly.
The actual app is in App/frontend/streamlit_app.py
"""

import sys
import os

# Add the frontend directory to Python path
frontend_dir = os.path.join(os.path.dirname(__file__), 'App', 'frontend')
sys.path.insert(0, frontend_dir)

# Change to the frontend directory so relative imports work
os.chdir(frontend_dir)

# Import and run the actual app
from streamlit_app import main

if __name__ == "__main__":
    main()
