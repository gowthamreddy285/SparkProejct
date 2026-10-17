# 📊 Charts Summary - All Notebooks

This document lists all the charts included in each Jupyter notebook.

---

## 1️⃣ Data Exploration Notebook (1_Data_Exploration.ipynb)

### ✅ Chart 1: Event Distribution
- **Type**: Bar chart
- **Shows**: Count of each event type (view, addtocart, transaction)
- **Explanation**: Most events are views - customers browse a lot

### ✅ Chart 2: Visitor Engagement Distribution
- **Type**: Bar chart
- **Shows**: How many events each visitor generates
- **Explanation**: Most visitors have 1-5 events, some are very engaged (20+)

### ✅ Chart 3: E-commerce Activity by Hour
- **Type**: Line chart
- **Shows**: Shopping activity across 24 hours
- **Explanation**: Identifies peak shopping hours for marketing

### ✅ Chart 4: Top 10 Most Viewed Products
- **Type**: Horizontal bar chart
- **Shows**: Products with most views
- **Explanation**: Helps identify popular products for inventory planning

**Total Charts: 4**

---

## 2️⃣ Logistic Regression Notebook (2_Model_Logistic_Regression.ipynb)

### ✅ Chart 1: Confusion Matrix
- **Type**: Heatmap
- **Shows**: Prediction accuracy breakdown
- **Explanation**: Diagonal values show correct predictions

### ✅ Chart 2: Model Performance Metrics
- **Type**: Bar chart
- **Shows**: Accuracy, Precision, Recall, F1-Score
- **Explanation**: High accuracy but low recall - model is conservative

### ✅ Chart 3: Prediction Distribution
- **Type**: Bar chart
- **Shows**: How many predicted "Purchase" vs "No Purchase"
- **Explanation**: Model predicts very few purchases (conservative)

**Total Charts: 3**

---

## 3️⃣ Random Forest Notebook (3_Model_Random_Forest.ipynb)

### ✅ Chart 1: Confusion Matrix
- **Type**: Heatmap (Green theme)
- **Shows**: Better balanced predictions than Logistic Regression
- **Explanation**: Random Forest correctly identifies more purchasers

### ✅ Chart 2: Feature Importance
- **Type**: Horizontal bar chart
- **Shows**: Which features matter most
- **Explanation**: Cart count is strongest predictor - visitors who add items are likely to purchase

### ✅ Chart 3: Model Performance Comparison
- **Type**: Bar chart
- **Shows**: All 4 metrics (Accuracy, Precision, Recall, F1)
- **Explanation**: Excels in recall (70%) while maintaining high precision (94%)

**Total Charts: 3**

---

## 4️⃣ Decision Tree Notebook (4_Model_Decision_Tree.ipynb)

### ✅ Chart 1: Confusion Matrix
- **Type**: Heatmap (Orange theme)
- **Shows**: Perfect precision predictions
- **Explanation**: When it predicts purchase, it's 100% correct (but conservative)

### ✅ Chart 2: Performance Metrics
- **Type**: Bar chart
- **Shows**: All metrics with perfect precision (1.0)
- **Explanation**: Prioritizes accuracy over catching all purchases

### ✅ Chart 3: Feature Importance
- **Type**: Horizontal bar chart with tree info
- **Shows**: Decision tree's feature usage
- **Explanation**: Cart count dominates - strong purchase signal

**Total Charts: 3**

---

## 5️⃣ Gradient Boosted Trees Notebook (5_Model_Gradient_Boosted_Trees.ipynb) 🏆

### ✅ Chart 1: Confusion Matrix
- **Type**: Heatmap (Purple theme)
- **Shows**: Best confusion matrix of all models
- **Explanation**: 79% recall with 98% precision - best balance!

### ✅ Chart 2: Feature Importance
- **Type**: Horizontal bar chart
- **Shows**: Boosting's feature discoveries
- **Explanation**: Cart behavior is strongest purchase signal (confirmed by boosting)

### ✅ Chart 3: All Models Comparison 🏆
- **Type**: Grouped bar chart with winner star
- **Shows**: Accuracy and F1-Score for all 4 models
- **Explanation**: Gradient Boosting wins with best F1-Score (0.88) and accuracy (99.81%)

**Total Charts: 3**

---

## 📈 Grand Total

| Notebook | Charts | Status |
|----------|--------|--------|
| 1. Data Exploration | 4 | ✅ |
| 2. Logistic Regression | 3 | ✅ |
| 3. Random Forest | 3 | ✅ |
| 4. Decision Tree | 3 | ✅ |
| 5. Gradient Boosted Trees | 3 | ✅ |
| **TOTAL** | **16** | ✅ |

---

## ✅ Requirements Met

**Review 3 Requirement**: "Include at least 3 graphs/charts"

✅ **Data Exploration**: 4 charts (exceeds requirement)  
✅ **Logistic Regression**: 3 charts  
✅ **Random Forest**: 3 charts  
✅ **Decision Tree**: 3 charts  
✅ **Gradient Boosted Trees**: 3 charts  

**Total**: 16 meaningful charts with explanations across all notebooks!

---

## 🎨 Chart Types Used

1. **Bar Charts** - Event distribution, metrics comparison
2. **Horizontal Bar Charts** - Top products, feature importance
3. **Line Charts** - Hourly activity patterns
4. **Heatmaps** - Confusion matrices (color-coded by model)
5. **Grouped Bar Charts** - Model comparison

All charts include:
- ✅ Clear titles
- ✅ Axis labels
- ✅ Value annotations
- ✅ Color coding
- ✅ Explanations in markdown

---

**Last Updated**: October 17, 2026  
**Status**: COMPLETE ✅
