#!/bin/bash
# Launch Jupyter Notebook with virtual environment

cd "$(dirname "$0")"

# Check if .venv exists
if [ ! -d ".venv" ]; then
    echo "Virtual environment not found!"
    echo "Running setup first..."
    bash setup_jupyter.sh
fi

echo "=========================================="
echo "Launching Jupyter Notebook"
echo "=========================================="
echo ""
echo "Notebooks available:"
echo "  1. Data Exploration (6 charts)"
echo "  2. Logistic Regression (3 charts)"
echo "  3. Random Forest (3 charts)"
echo "  4. Decision Tree (3 charts)"
echo "  5. Gradient Boosted Trees (4 charts)"
echo ""
echo "Opening in your browser..."
echo ""
echo "To stop: Press Ctrl+C"
echo "=========================================="
echo ""

# Activate virtual environment and launch Jupyter
source .venv/bin/activate
cd notebooks
jupyter notebook
