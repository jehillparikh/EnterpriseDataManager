from app import app, setup_api

if __name__ == "__main__":
    # Setup API routes
    setup_api(app)
    # Run the Flask application
    app.run(host="0.0.0.0", port=5000, debug=True)
