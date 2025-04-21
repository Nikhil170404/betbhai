#!/bin/bash
# Start script for Cricket Odds API on Render
# This script installs Chrome, ChromeDriver, and starts the application

# Exit immediately if a command exits with a non-zero status
set -e

echo "Starting setup process for Cricket Odds API..."

# Install Chrome dependencies
echo "Installing Chrome dependencies..."
apt-get update
apt-get install -y wget gnupg unzip curl xvfb

# Install Chrome browser
echo "Installing Google Chrome..."
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list
apt-get update
apt-get install -y google-chrome-stable

# Get Chrome version and corresponding ChromeDriver
echo "Setting up ChromeDriver..."
CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d. -f1)
echo "Detected Chrome version: $CHROME_VERSION"

# Find matching ChromeDriver version
CHROMEDRIVER_VERSION=$(curl -s "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION")
echo "Using ChromeDriver version: $CHROMEDRIVER_VERSION"

# Download and install ChromeDriver
wget -q -O /tmp/chromedriver.zip "https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip"
unzip -o /tmp/chromedriver.zip -d /usr/local/bin/
chmod +x /usr/local/bin/chromedriver
rm /tmp/chromedriver.zip

# Check installation
echo "ChromeDriver installed at: $(which chromedriver)"
echo "ChromeDriver version: $(chromedriver --version)"

# Create data directory
mkdir -p data

# Set up virtual environment if needed (commented out - uncomment if needed)
# python -m venv venv
# source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Get PORT from environment or default to 10000
PORT="${PORT:-10000}"
echo "Starting application on port $PORT"

# Run the FastAPI application with uvicorn
exec python -m uvicorn app:app --host 0.0.0.0 --port $PORT
