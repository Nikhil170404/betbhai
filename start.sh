#!/bin/bash
# Create required directories
mkdir -p data
mkdir -p match_logs
mkdir -p debug_html
# Set environment variable for Render detection
export RENDER=true
# Install Chrome directly in the start script to ensure it's available
echo "Installing Chrome..."
apt-get update
apt-get install -y wget gnupg
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list
apt-get update
apt-get install -y google-chrome-stable
# Verify Chrome installation
echo "Chrome version:"
google-chrome --version
# Manually download and set up ChromeDriver
echo "Setting up ChromeDriver..."
CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d. -f1)
CHROMEDRIVER_VERSION=$(curl -s "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION")
wget -q "https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip"
unzip chromedriver_linux64.zip
chmod +x chromedriver
mv chromedriver /usr/local/bin/
echo "ChromeDriver installed at: $(which chromedriver)"
echo "ChromeDriver version: $(chromedriver --version)"
# Set environment variables for better performance
export PYTHONUNBUFFERED=1
export CHROME_BIN=/usr/bin/google-chrome
export CHROME_PATH=/usr/bin/google-chrome
export WEB_CONCURRENCY=1  # Ensure only one worker to avoid multiple scrapers
# Increase system limits for Chrome
echo "Increasing system limits for Chrome"
ulimit -n 4096
# Configure Chrome for better stability in containerized environments
echo "Configuring Chrome for container environment"
mkdir -p /tmp/chrome-user-data
chmod 777 /tmp/chrome-user-data
# Start the application with proper logging and extended timeouts
echo "Starting application on port $PORT"
exec uvicorn app:app --host 0.0.0.0 --port $PORT --timeout-keep-alive 300 
