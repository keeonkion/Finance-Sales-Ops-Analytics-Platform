import os
from dotenv import load_dotenv
import mlflow

def setup_mlflow():
    # load .env into process env vars (PGHOST etc.)
    load_dotenv()

    # local tracking (stored in ./mlruns by default)
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "file:./mlruns")
    mlflow.set_tracking_uri(tracking_uri)
    return tracking_uri

    