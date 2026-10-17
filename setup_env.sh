#!/bin/bash
# Setup environment for RetailIQ project

cd "$(dirname "$0")"

echo "=========================================="
echo "RetailIQ: Environment Setup"
echo "=========================================="
echo ""

# Check Python
echo "Checking Python..."
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python3 is not installed!"
    exit 1
fi
python3 --version

# Check Java
echo ""
echo "Checking Java..."
if ! command -v java &> /dev/null; then
    echo "ERROR: Java is not installed!"
    echo "Please install Java 17 or later"
    exit 1
fi
java -version

# Check SBT
echo ""
echo "Checking SBT..."
if ! command -v sbt &> /dev/null; then
    echo "ERROR: SBT is not installed!"
    echo "Please install SBT using:"
    echo "  brew install sbt"
    exit 1
fi
sbt --version | head -2

# Install Python dependencies
echo ""
echo "Installing Python dependencies..."
pip3 install --user pyspark==3.5.1 numpy pandas matplotlib

if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "Environment setup completed!"
    echo "=========================================="
    echo ""
    echo "You can now run:"
    echo "  ./run_all.sh       - Run everything"
    echo "  ./run_scala.sh     - Run Scala only"
    echo "  ./run_pyspark_ml.sh - Run PySpark ML only"
    echo "  ./run_visualizations.sh - Run visualizations only"
    echo ""
else
    echo "ERROR: Failed to install Python dependencies!"
    exit 1
fi
