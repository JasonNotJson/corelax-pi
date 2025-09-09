#!/bin/bash
# Corelax Pi Service Deployment Script
# Usage: curl -sSL https://raw.githubusercontent.com/Blvcksmiths/corelax-flutter/main/scripts/realtime-test/deploy-pi.sh | sudo bash

set -e

echo "[INFO] Starting Corelax Pi service deployment..."

# Create directory
echo "[INFO] Creating service directory..."
sudo mkdir -p /opt/corelax-service-pi

# Download the clean service file
echo "[INFO] Downloading service file from GitHub..."
sudo curl -sSL -o /opt/corelax-service-pi/service-pi-listener.mjs \
  https://raw.githubusercontent.com/JasonNotJson/corelax-pi/main/service-pi-listener-clean.mjs

# Install dependencies
echo "[INFO] Installing Node.js dependencies..."
cd /opt/corelax-service-pi
sudo npm init -y --silent
sudo npm install mqtt ws @supabase/supabase-js dotenv --silent

# Set permissions
echo "[INFO] Setting permissions..."
sudo chown -R root:root /opt/corelax-service-pi
sudo chmod 755 /opt/corelax-service-pi
sudo chmod 644 /opt/corelax-service-pi/*

echo "[SUCCESS] Deployment complete!"
echo ""
echo "Next steps:"
echo "1. Create .env file: sudo nano /opt/corelax-service-pi/.env"
echo "2. Add your environment variables:"
echo "   SUPABASE_URL=your_url"
echo "   SUPABASE_ANON_KEY=your_key"
echo "   PI_EMAIL=your_email"
echo "   PI_PASSWORD=your_password"
echo "3. Test run: cd /opt/corelax-service-pi && node service-pi-listener.mjs"
echo "4. Setup systemd service for auto-start"