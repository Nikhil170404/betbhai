#!/bin/bash

# Create data directory if it doesn't exist
mkdir -p data

# Set environment variable for Render detection
export RENDER=true

# If running on Render, try to install Chrome (will be ignored if already installed)
if [ "$RENDER" = "true" ]; then
  echo "Running on Render, checking Chrome installation..."
  
  if ! command -v google-chrome &> /dev/null; then
    echo "Chrome not found, installing..."
    apt-get update
    apt-get install -y wget gnupg
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list
    apt-get update
    apt-get install -y google-chrome-stable
  else
    echo "Chrome already installed: $(google-chrome --version)"
  fi
fi

# Start the FastAPI app with uvicorn
echo "Starting cricket odds API on port $PORT"
uvicorn app:app --host 0.0.0.0 --port $PORT
