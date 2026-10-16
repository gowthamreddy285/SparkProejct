from pathlib import Path
import math
import re

from flask import Flask, jsonify, render_template, request
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel

# --------------------------------------------------
# Paths
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
EVIDENCE_DIR = BASE_DIR / "review_evidence"

MODEL_PATH = OUTPUT_DIR / "enhanced_purchase_model"
THRESHOLD_PATH = OUTPUT_DIR / "enhanced_purchase_threshold.txt"
COMPARISON_PATH = EVIDENCE_DIR / "enhanced_model_comparison.txt"

app = Flask(__name__)

# --------------------------------------------------
# Spark and saved prediction model
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("RetailIQPurchaseWebsite")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

model = PipelineModel.load(MODEL_PATH.resolve().as_uri())

# --------------------------------------------------
# Model comparison metrics
# --------------------------------------------------
# These are the metrics verified in review_evidence/original_ml_run.txt.
# They are kept separate from the enhanced model used by /api/predict.
VERIFIED_MODEL_RESULTS = [
    {
        "name": "Logistic Regression",
        "accuracy": 0.9919,
        "precision": 0.5051,
        "recall": 0.0432,
        "f1": 0.0796,
        "pr_auc": None,
        "threshold": None,
        "tn": 278722,
        "fp": 97,
        "fn": 2192,
        "tp": 99,
    },
    {
        "name": "Random Forest",
        "accuracy": 0.9967,
        "precision": 0.9240,
        "recall": 0.6478,
        "f1": 0.7616,
        "pr_auc": None,
        "threshold": None,
        "tn": 278697,
        "fp": 122,
        "fn": 807,
        "tp": 1484,
    },
   {
        "name": "Decision Tree",
        "accuracy": 0.9941,
        "precision": 0.8524,
        "recall": 0.3352,
        "f1": 0.4812,
        "pr_auc": None,
        "threshold": None,
        "tn": None,
        "fp": None,
        "fn": None,
        "tp": None,
    },
    {
        "name": "Gradient-Boosted Tree",
        "accuracy": 0.9977,
        "precision": 0.9774,
        "recall": 0.7351,
        "f1": 0.8391,
        "pr_auc": None,
        "threshold": None,
        "tn": None,
        "fp": None,
        "fn": None,
        "tp": None,
    },
]


def read_model_results():
    """Return the verified original Logistic Regression and Random Forest results."""
    # Return fresh dictionaries so request handlers cannot mutate the constants.
    return [dict(result) for result in VERIFIED_MODEL_RESULTS]


def read_dashboard_metrics():
    """Return the best reported model's verified metrics for the summary cards."""
    results = read_model_results()
    best = max(results, key=lambda result: result["f1"])
    metrics = dict(best)
    metrics["available"] = True
    return metrics


# --------------------------------------------------
# Pages and metrics API
# --------------------------------------------------
@app.route("/")
def index():
    metrics = read_dashboard_metrics()
    model_results = read_model_results()
    return render_template(
        "index.html",
        metrics=metrics,
        model_results=model_results,
    )


@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "model_loaded": model is not None,
    })


@app.route("/api/metrics")
def metrics_api():
    """Return verified original model metrics and the best-model summary."""
    return jsonify({
        "metrics": read_dashboard_metrics(),
        "models": read_model_results(),
    })


# --------------------------------------------------
# Live prediction endpoint (enhanced saved model)
# --------------------------------------------------
@app.route("/api/predict", methods=["POST"])
def predict():
    payload = request.get_json(silent=True) or {}

    integer_fields = [
        "views",
        "carts",
        "total_events",
        "unique_items",
        "unique_viewed",
        "unique_carted",
    ]
    numeric_fields = [
        "days_active",
        "hours_since_activity",
    ]

    try:
        values = {}
        for field in integer_fields:
            number = float(payload[field])
            if not math.isfinite(number) or not number.is_integer() or number < 0:
                raise ValueError(f"{field} must be a non-negative integer")
            values[field] = int(number)

        for field in numeric_fields:
            number = float(payload[field])
            if not math.isfinite(number) or number < 0:
                raise ValueError(f"{field} must be non-negative")
            values[field] = number

    except (KeyError, TypeError, ValueError) as exc:
        return jsonify({"error": f"Invalid input: {exc}"}), 400

    if values["total_events"] < values["views"] + values["carts"]:
        return jsonify({
            "error": "Total events must be at least views plus cart events."
        }), 400

    if values["unique_items"] > values["total_events"]:
        return jsonify({"error": "Unique items cannot exceed total events."}), 400

    if values["unique_viewed"] > values["views"]:
        return jsonify({"error": "Unique viewed items cannot exceed view events."}), 400

    if values["unique_carted"] > values["carts"]:
        return jsonify({"error": "Unique carted items cannot exceed cart events."}), 400

    # Keep feature names and calculations aligned with the enhanced pipeline.
    row = {
        "log_views": math.log1p(values["views"]),
        "log_carts": math.log1p(values["carts"]),
        "log_total": math.log1p(values["total_events"]),
        "log_unique_items": math.log1p(values["unique_items"]),
        "log_unique_viewed": math.log1p(values["unique_viewed"]),
        "log_unique_carted": math.log1p(values["unique_carted"]),
        "log_days_active": math.log1p(values["days_active"]),
        "log_recency": math.log1p(values["hours_since_activity"]),
        "has_cart": 1.0 if values["carts"] > 0 else 0.0,
        "cart_view_ratio": values["carts"] / (values["views"] + 1.0),
        "events_per_active_day": values["total_events"] / (values["days_active"] + 1.0),
    }

    try:
        prediction_row = model.transform(spark.createDataFrame([row])).first()
        score = float(prediction_row["probability"][1])

        try:
            threshold = float(THRESHOLD_PATH.read_text().strip())
            if not math.isfinite(threshold) or not 0 <= threshold <= 1:
                raise ValueError("Threshold must be between 0 and 1.")
        except (ValueError, OSError):
            return jsonify({
                "error": "The saved prediction threshold is unavailable or invalid."
            }), 503

        predicted_buyer = score >= threshold
        return jsonify({
            "score": score,
            "score_percent": round(score * 100, 2),
            "threshold": threshold,
            "prediction": int(predicted_buyer),
            "prediction_label": (
                "Higher purchase likelihood" if predicted_buyer
                else "Lower purchase likelihood"
            ),
            "note": (
                "This is an uncalibrated model score, not a guaranteed "
                "real-world purchase probability."
            ),
        })

    except Exception:
        app.logger.exception("Prediction failed")
        return jsonify({
            "error": "Prediction failed. Check the Flask terminal for details."
        }), 500


# --------------------------------------------------
# Dataset statistics API
# --------------------------------------------------
_dataset_stats_cache = None


@app.route("/api/dataset", methods=["GET"])
def dataset_stats():
    global _dataset_stats_cache

    if _dataset_stats_cache is not None:
        return jsonify(_dataset_stats_cache)

    try:
        dataset_path = (BASE_DIR / "data" / "events.csv").resolve()
        if not dataset_path.is_file():
            return jsonify({"error": "Dataset file not found."}), 404

        events_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(dataset_path.as_uri())
        )

        required_columns = {
            "timestamp", "visitorid", "event", "itemid", "transactionid"
        }
        if not required_columns.issubset(set(events_df.columns)):
            return jsonify({
                "error": "Dataset columns do not match the expected schema."
            }), 500

        total_events = events_df.count()
        unique_visitors = events_df.select("visitorid").distinct().count()
        event_counts = {
            row["event"]: row["count"]
            for row in events_df.groupBy("event").count().collect()
        }

        _dataset_stats_cache = {
            "file": "data/events.csv",
            "file_size_mb": round(dataset_path.stat().st_size / (1024 * 1024), 2),
            "total_events": total_events,
            "unique_visitors": unique_visitors,
            "columns": events_df.columns,
            "event_counts": event_counts,
            "status": "success",
        }
        return jsonify(_dataset_stats_cache)

    except Exception:
        app.logger.exception("Dataset statistics failed")
        return jsonify({
            "error": "Could not calculate dataset statistics. Check the Flask terminal."
        }), 500


# --------------------------------------------------
# Next-Best-Product API
# --------------------------------------------------
_products_cache = None


@app.route("/api/products", methods=["GET"])
def product_recommendations():
    global _products_cache

    if _products_cache is not None:
        return jsonify(_products_cache)

    try:
        dataset_path = (BASE_DIR / "data" / "events.csv").resolve()
        if not dataset_path.is_file():
            return jsonify({"error": "Dataset file not found."}), 404

        events_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(dataset_path.as_uri())
        )

        products_df = (
            events_df
            .filter(
                F.col("itemid").isNotNull()
                & F.col("event").isin("view", "addtocart", "transaction")
            )
            .groupBy("itemid")
            .agg(F.count("*").alias("interactions"))
            .orderBy(F.desc("interactions"), F.asc("itemid"))
            .limit(3)
            .collect()
        )

        if not products_df:
            return jsonify({"products": [], "status": "success"})

        max_interactions = max(row["interactions"] for row in products_df)
        _products_cache = {
            "status": "success",
            "method": "Popularity baseline",
            "products": [
                {
                    "itemid": str(row["itemid"]),
                    "interactions": int(row["interactions"]),
                    "relevance_score": round(
                        row["interactions"] / max_interactions * 100, 2
                    ),
                }
                for row in products_df
            ],
            "note": (
                "Relevance scores are normalized interaction counts, "
                "not purchase probabilities or personalized recommendations."
            ),
        }
        return jsonify(_products_cache)

    except Exception:
        app.logger.exception("Product recommendations failed")
        return jsonify({
            "error": "Could not calculate product recommendations."
        }), 500


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
