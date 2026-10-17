# RetailIQ Jupyter Notebooks

**Interactive notebooks for data exploration and machine learning**

## 📚 Notebooks Overview

### 1. Data Exploration (`1_Data_Exploration.ipynb`)
- Load and explore 2.7M e-commerce events
- Visualize event distribution
- Analyze visitor engagement patterns
- Identify hourly shopping trends
- Find top products
- **Time**: ~5 minutes

### 2. Model 1: Logistic Regression (`2_Model_Logistic_Regression.ipynb`)
- Simple linear classification model
- Feature engineering
- Train-test split
- Model evaluation
- Confusion matrix visualization
- **Result**: 99.15% accuracy, 0.0620 F1-score

### 3. Model 2: Random Forest (`3_Model_Random_Forest.ipynb`)
- Ensemble of 50 decision trees
- Improved performance over logistic regression
- Feature importance analysis
- **Result**: 99.70% accuracy, 0.8021 F1-score

### 4. Model 3: Decision Tree (`4_Model_Decision_Tree.ipynb`)
- Single tree classification
- Perfect precision (100%)
- Tree depth and nodes analysis
- **Result**: 99.61% accuracy, 0.7041 F1-score

### 5. Model 4: Gradient Boosted Trees (`5_Model_Gradient_Boosted_Trees.ipynb`) 🏆
- Sequential tree building with error correction
- **BEST MODEL!**
- Comprehensive model comparison
- **Result**: 99.81% accuracy, 0.8766 F1-score

---

## 🚀 How to Run

### Option 1: Launch Jupyter (Recommended)
```bash
cd /Users/kireeti/Desktop/Projects/SparkProejct
./launch_jupyter.sh
```

Then open your browser and navigate through the notebooks in order.

### Option 2: Manual Launch
```bash
cd notebooks
jupyter notebook
```

---

## 📊 What You'll Learn

### Data Skills:
- ✓ Load large datasets with PySpark
- ✓ Aggregate and transform data
- ✓ Create meaningful visualizations
- ✓ Extract business insights

### ML Skills:
- ✓ Feature engineering
- ✓ Train-test splitting
- ✓ Build 4 different ML models
- ✓ Evaluate with multiple metrics
- ✓ Compare model performance
- ✓ Visualize confusion matrices

### Algorithms Covered:
1. **Logistic Regression** - Linear baseline
2. **Random Forest** - Parallel ensemble
3. **Decision Tree** - Interpretable tree
4. **Gradient Boosting** - Sequential ensemble (BEST)

---

## 🎯 Learning Path

**Beginner**: Start with Notebook 1 (Data Exploration)
- Understand the dataset
- See basic PySpark operations
- Learn visualization techniques

**Intermediate**: Continue to Notebooks 2-4
- Compare different ML algorithms
- Understand trade-offs
- Learn evaluation metrics

**Advanced**: Finish with Notebook 5
- Master ensemble methods
- Understand boosting
- See complete model comparison

---

## 💡 Key Takeaways

### Model Performance Ranking:
| Rank | Model | F1-Score | When to Use |
|------|-------|----------|-------------|
| 🥇 | Gradient Boosting | 0.8766 | **Production** (best overall) |
| 🥈 | Random Forest | 0.8021 | Fast training needed |
| 🥉 | Decision Tree | 0.7041 | Interpretability required |
| 4th | Logistic Regression | 0.0620 | Quick baseline |

### Business Impact:
- **79% recall**: Identify most potential buyers
- **98% precision**: Minimize wasted marketing
- **Real-time predictions**: Enable instant personalization

---

## 📝 Notes

- All notebooks use the same train/test split (seed=42) for fair comparison
- Visualizations are displayed inline
- Each notebook is self-contained and can run independently
- Estimated total runtime: ~20-30 minutes for all notebooks

---

## 🔧 Requirements

Already installed if you ran `setup_env.sh`:
- Python 3.10+
- PySpark 3.5.1
- Jupyter Notebook
- Matplotlib
- Pandas

---

## 🎓 For Presentation

**Best Order to Show:**
1. Start with Notebook 1 - show data understanding
2. Show Notebook 2 (Logistic Regression) - establish baseline
3. Jump to Notebook 5 (Gradient Boosting) - show best model
4. Display final comparison chart

**Key Slides to Screenshot:**
- Event distribution chart
- Confusion matrices
- Model comparison chart
- Final ranking table

---

**Happy Learning!** 🚀
