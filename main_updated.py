from app import app
from new_api import setup_routes
import logging

logging.basicConfig(level=logging.INFO)

# Setup API routes
setup_routes(app)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)