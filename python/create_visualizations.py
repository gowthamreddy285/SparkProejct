
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
from pathlib import Path

OUT = Path("/home/satwiqreddy/SparkProject/visualizations")
OUT.mkdir(parents=True, exist_ok=True)

# 1. Event type distribution
event_names = ["Views", "Add to cart", "Transactions"]
event_counts = [2664312, 69332, 22457]

plt.figure(figsize=(8, 5))
plt.bar(event_names, event_counts)
plt.title("Retailrocket Event Type Distribution")
plt.ylabel("Number of events")
plt.tight_layout()
plt.savefig(OUT / "event_distribution.png", dpi=150)
plt.close()

# 2. Visitor label imbalance
labels = ["Non-purchasers", "Purchasers"]
label_counts = [606154, 583]

plt.figure(figsize=(7, 5))
plt.bar(labels, label_counts)
plt.title("Visitor Purchase Label Distribution")
plt.ylabel("Number of visitors")
plt.tight_layout()
plt.savefig(OUT / "label_distribution.png", dpi=150)
plt.close()

# 3. Threshold comparison
thresholds = [0.1, 0.3, 0.5, 0.7, 0.9]
precision = [0.0009, 0.0009, 0.0091, 0.0730, 0.1429]
recall = [1.0000, 1.0000, 0.2411, 0.1161, 0.0625]
f1 = [0.0018, 0.0018, 0.0176, 0.0897, 0.0870]

plt.figure(figsize=(8, 5))
plt.plot(thresholds, precision, marker="o", label="Precision")
plt.plot(thresholds, recall, marker="o", label="Recall")
plt.plot(thresholds, f1, marker="o", label="F1-score")
plt.title("Model Metrics by Prediction Threshold")
plt.xlabel("Threshold")
plt.ylabel("Score")
plt.ylim(0, 1.05)
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(OUT / "threshold_comparison.png", dpi=150)
plt.close()

print("Visualizations saved to:", OUT)
for file in sorted(OUT.glob("*.png")):
    print(file.name)
