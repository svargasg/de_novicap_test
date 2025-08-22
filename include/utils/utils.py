import logging
from datetime import datetime, timezone

from ydata_profiling import ProfileReport

from include.common.params import GlobalParams

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def generate_profile_report(df):
    _params = GlobalParams()
    sufix_ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")[:-3]
    path = f"{_params.TMP_PROFILING}_{sufix_ts}.html"
    logger.info("Generating profiling report...")
    profile = ProfileReport(df, title="Novicap Profiling Report")
    profile.to_file(path)
    logger.info(f"Report was exported as html in {path}")
    return path
