#!/bin/bash

# UAV Data Collector Installation Script
echo "Installing UAV Data Collector..."

# Download the setup script
curl -o setup.sh https://raw.githubusercontent.com/yourusername/uav-data-collector/main/setup.sh
chmod +x setup.sh

# Run the setup script
./setup.sh

echo "Installation complete!"

