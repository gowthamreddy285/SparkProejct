#!/bin/bash
# Setup Python virtual environment for Jupyter notebooks

cd "$(dirname "$0")"

echo "=========================================="
echo "Setting up Jupyter Environment"
echo "=========================================="
echo ""

# Create virtual environment
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip > /dev/null 2>&1

# Install required packages
echo ""
echo "Installing packages..."
echo "  - pyspark"
pip install pyspark==3.5.1 > /dev/null 2>&1
echo "  - jupyter"
pip install jupyter > /dev/null 2>&1
echo "  - matplotlib"
pip install matplotlib > /dev/null 2>&1
echo "  - pandas"
pip install pandas > /dev/null 2>&1
echo "  - numpy"
pip install numpy > /dev/null 2>&1

# Install Jupyter kernel
echo ""
echo "Setting up Jupyter kernel..."
python -m ipykernel install --user --name=sparkproject --display-name="SparkProject"

echo ""
echo "=========================================="
echo "✓ Setup Complete!"
echo "=========================================="
echo ""
echo "To launch Jupyter:"
echo "  1. source .venv/bin/activate"
echo "  2. jupyter notebook notebooks/"
echo ""
echo "Or simply run:"
echo "  ./launch_jupyter.sh"
echo ""
