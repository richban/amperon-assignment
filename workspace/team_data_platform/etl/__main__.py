import logging
from etl.pipeline import run_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("Starting Weather Data Pipeline")

# Run the pipeline
run_pipeline()

logger.info("Pipeline execution completed")
