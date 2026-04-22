# Entrypoint for Vercel deployment
import os
import sys
from pathlib import Path

# Ensure src is in path and working directory is correct
app_root = Path(__file__).parent
sys.path.insert(0, str(app_root))
os.chdir(app_root)

from src.webapp import app

if __name__ == "__main__":
    app.run(debug=False)
