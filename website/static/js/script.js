
document.addEventListener("DOMContentLoaded", async () => {
  const form = document.getElementById("prediction-form");
  const button = document.getElementById("predict-button");
  const emptyResult = document.getElementById("empty-result");
  const resultPanel = document.getElementById("prediction-result");
  const errorMessage = document.getElementById("error-message");

  // Load the latest metrics from the Flask API for the chart.
  try {
    const response = await fetch("/api/metrics");
    if (!response.ok) {
      throw new Error("Could not load model metrics.");
    }

    const payload = await response.json();
    const results = payload.models || [];
    const canvas = document.getElementById("metrics-chart");

    if (canvas && window.Chart && results.length > 0) {
      new Chart(canvas, {
        type: "bar",
        data: {
          labels: results.map((item) => item.name),
          datasets: [
            {
              label: "Precision",
              data: results.map((item) => item.precision * 100)
            },
            {
              label: "Recall",
              data: results.map((item) => item.recall * 100)
            },
            {
              label: "F1-score",
              data: results.map((item) => item.f1 * 100)
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            y: {
              beginAtZero: true,
              max: 100,
              title: {
                display: true,
                text: "Score (%)"
              }
            }
          },
          plugins: {
            legend: {
              position: "bottom"
            },
            tooltip: {
              callbacks: {
                label: (context) =>
                  `${context.dataset.label}: ${context.parsed.y.toFixed(2)}%`
              }
            }
          }
        }
      });
    }
  } catch (error) {
    console.error("Metrics chart error:", error);
  }

  // Reset the prediction panel.
  function clearResult() {
    emptyResult?.classList.remove("hidden");
    resultPanel?.classList.add("hidden");
    errorMessage?.classList.add("hidden");
    if (errorMessage) errorMessage.textContent = "";
  }

  // Submit visitor activity to Flask.
  if (form) {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();

      errorMessage?.classList.add("hidden");
      if (errorMessage) errorMessage.textContent = "";

      const formData = new FormData(form);
      const payload = {};

      for (const [key, value] of formData.entries()) {
        payload[key] = value.trim() === "" ? NaN : Number(value);
      }

      if (button) {
        button.disabled = true;
        button.textContent = "Analyzing...";
      }

      try {
        const response = await fetch("/api/predict", {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify(payload)
        });

        const result = await response.json();

        if (!response.ok) {
          throw new Error(result.error || "Prediction request failed.");
        }

        if (emptyResult) emptyResult.classList.add("hidden");
        if (resultPanel) resultPanel.classList.remove("hidden");

        const scoreNumber = document.getElementById("score-number");
        const scoreBar = document.getElementById("score-bar");
        const predictionLabel = document.getElementById("prediction-label");
        const thresholdValue = document.getElementById("threshold-value");
        const predictionNote = document.getElementById("prediction-note");

        if (scoreNumber) {
          scoreNumber.textContent = `${result.score_percent.toFixed(2)}%`;
        }

        if (scoreBar) {
          scoreBar.style.width =
            `${Math.max(0, Math.min(100, result.score_percent))}%`;
        }

        if (predictionLabel) {
          predictionLabel.textContent = result.prediction_label;
          predictionLabel.classList.toggle(
            "positive",
            result.prediction === 1
          );
          predictionLabel.classList.toggle(
            "negative",
            result.prediction === 0
          );
        }

        if (thresholdValue) {
          thresholdValue.textContent = Number(result.threshold).toFixed(2);
        }

        if (predictionNote) {
          predictionNote.textContent = result.note || "";
        }
      } catch (error) {
        if (errorMessage) {
          errorMessage.textContent = error.message;
          errorMessage.classList.remove("hidden");
        }
      } finally {
        if (button) {
          button.disabled = false;
          button.innerHTML =
            'Predict Purchase Likelihood <span>→</span>';
        }
      }
    });

    form.addEventListener("reset", () => {
      window.setTimeout(clearResult, 0);
    });
  }
});
