import os

from dagster import Definitions, load_assets_from_modules

from .assets import dno_smart_meter_files
from .resources.raw_files import RawFilesResource
from .resources.ssen import LiveSSENAPIClient
from .sensors import ssen_lv_feeder_files_sensor

all_assets = load_assets_from_modules([dno_smart_meter_files])

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = os.path.join(CURRENT_DIR, "..", "data")
DEPLOYMENT_NAME = os.getenv("DAGSTER_DEPLOYMENT", "dev_local")
FIXTURE_DIR = os.path.join(CURRENT_DIR, "..", "weave_tests", "fixtures")

resources = {
    "dev_local": {
        "raw_files_resource": RawFilesResource(
            url=f"file://{os.path.join(DATA_DIR, "raw")}"
        ),
        "ssen_api_client": LiveSSENAPIClient(
            available_files_url="https://ssen-smart-meter-prod.datopian.workers.dev/LV_FEEDER_USAGE/"
        ),
    },
    "dev_cloud": {
        "raw_files_resource": RawFilesResource(url="s3://weave.energy-dev/data/raw"),
        "ssen_api_client": LiveSSENAPIClient(
            available_files_url="https://ssen-smart-meter-prod.datopian.workers.dev/LV_FEEDER_USAGE/"
        ),
    },
    "prod": {
        "raw_files_resource": RawFilesResource(url="s3://weave.energy/data/raw"),
        "ssen_api_client": LiveSSENAPIClient(
            available_files_url="https://ssen-smart-meter-prod.datopian.workers.dev/LV_FEEDER_USAGE/"
        ),
    },
}


defs = Definitions(
    assets=all_assets,
    sensors=[ssen_lv_feeder_files_sensor],
    resources=resources[DEPLOYMENT_NAME],
)
