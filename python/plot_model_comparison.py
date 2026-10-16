
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

models = [
    "Basic Logistic\nRegression",
    "Basic Random\nForest",
    "Enhanced Logistic\nRegression",
    "Enhanced Random\nForest",
]

precision = [0.1299, 0.1165, 0.2889, 0.1358]
recall = [0.1136, 0.1364, 0.1327, 0.1122]
f1 = [0.1212, 0.1257, 0.1818, 0.1229]
pr_auc = [0.0564, 0.0305, 0.0387, 0.0533]

x = np.arange(len(models))
width = 0.19

fig, ax = plt.subplots(figsize=(12, 6))

bars1 = ax.bar(x - 1.5 * width, precision, width, label="Precision")
bars2 = ax.bar(x - 0.5 * width, recall, width, label="Recall")
bars3 = ax.bar(x + 0.5 * width, f1, width, label="F1-score")
bars4 = ax.bar(x + 1.5 * width, pr_auc, width, label="PR-AUC")

ax.set_title("E-commerce Purchase Prediction: Model Comparison")
ax.set_ylabel("Score")
ax.set_xticks(x)
ax.set_xticklabels(models)
ax.set_ylim(0, 0.34)
ax.legend()
ax.grid(axis="y", alpha=0.25)

for bars in [bars1, bars2, bars3, bars4]:
    ax.bar_label(bars, fmt="%.3f", padding=2, fontsize=8)

fig.tight_layout()
fig.savefig("visualizations/model_comparison.png", dpi=300)
plt.close(fig)

print("Saved visualizations/model_comparison.png")
