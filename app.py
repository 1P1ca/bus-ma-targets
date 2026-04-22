# Entrypoint for Vercel deployment
from src.webapp import app

if __name__ == "__main__":
    app.run(debug=False)
