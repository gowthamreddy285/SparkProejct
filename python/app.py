import streamlit as st
import math
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
st.set_page_config(
    page_title="E-commerce Purchase Predictor",
    page_icon="🛒",
    layout="centered"
)

st.title("E-commerce Purchase Prediction")
st.write(
    "Enter a visitor's historical activity to estimate "
    "their likelihood of making a future purchase."
)

@st.cache_resource
def load_model():
    spark = (
        SparkSession.builder
        .appName("PurchasePredictionWebApp")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    model = PipelineModel.load(
    "/home/satwiqreddy/SparkProject/output/enhanced_purchase_model"
    )
    return spark, model

try:
    spark, model = load_model()

    views = st.number_input(
        "Historical product views",
        min_value=0, value=10, step=1
    )
    carts = st.number_input(
        "Historical add-to-cart events",
        min_value=0, value=1, step=1
    )
    total_events = st.number_input(
        "Total historical events",
        min_value=1, value=12, step=1
    )
    unique_items = st.number_input(
        "Unique items interacted with",
        min_value=0, value=8, step=1
    )
    unique_viewed = st.number_input(
        "Unique items viewed",
        min_value=0, value=8, step=1
    )
    unique_carted = st.number_input(
        "Unique items added to cart",
        min_value=0, value=1, step=1
    )
    days_active = st.number_input(
        "Days between first and last activity",
        min_value=0.0, value=2.0, step=1.0
    )
    hours_since_activity = st.number_input(
        "Hours since last activity",
        min_value=0.0, value=24.0, step=1.0
    )

    if st.button("Predict Purchase Likelihood", type="primary"):
        if carts > views + carts or unique_carted > unique_items:
            st.error("Please check the activity counts.")
        elif (
            unique_viewed > unique_items
            or unique_carted > carts
            or unique_viewed > views
            or total_events < views + carts
        ):
            st.error(
                "Please ensure unique counts and total events "
                "are consistent with the entered activity."
            )
        else:
            row = {
                "log_views": math.log1p(views),
                "log_carts": math.log1p(carts),
                "log_total": math.log1p(total_events),
                "log_unique_items": math.log1p(unique_items),
                "log_unique_viewed": math.log1p(unique_viewed),
                "log_unique_carted": math.log1p(unique_carted),
                "log_days_active": math.log1p(days_active),
                "log_recency": math.log1p(hours_since_activity),
                "has_cart": 1.0 if carts > 0 else 0.0
            }

            input_df = spark.createDataFrame([row])
            result = model.transform(input_df).first()
            probability = float(result["probability"][1])

            st.metric(
                "Estimated purchase probability",
                f"{probability * 100:.2f}%"
            )

            if probability >= 0.5:
                st.info(
                    "The model assigns this visitor a higher "
                    "purchase likelihood than its 0.5 probability threshold."
                )
            else:
                st.info(
                    "The model assigns this visitor a lower "
                    "purchase likelihood than its 0.5 probability threshold."
                )

            st.caption(
                "This is a model score, not a calibrated guarantee "
                "of a future purchase. Performance on the test set "
                "was limited."
            )

except Exception as e:
    st.error(
        "Could not load the saved Spark model. "
        "Complete Step 1 successfully first."
    )
    st.code(str(e))
