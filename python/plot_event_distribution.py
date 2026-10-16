import matplotlib.pyplot as plt

events = ["View", "Add to cart", "Transaction"]
counts = [2664312, 69332, 22457]

plt.figure(figsize=(8, 5))
bars = plt.bar(events, counts)

plt.title("Retailrocket E-commerce Event Distribution")
plt.xlabel("Event Type")
plt.ylabel("Number of Events")

plt.bar_label(bars, fmt="%.0f")
plt.tight_layout()
plt.savefig("visualizations/event_distribution.png", dpi=300)
plt.show()

