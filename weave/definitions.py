import os

from dagster import Definitions, load_assets_from_modules

from .assets import dno_lv_feeder_files
from .resources.raw_files import RawFilesResource
from .resources.ssen import LiveSSENAPIClient
from .sensors import ssen_lv_feeder_files_sensor

all_assets = load_assets_from_modules([dno_lv_feeder_files])

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = os.path.join(CURRENT_DIR, "..", "data")
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
    "branch": {
        "raw_files_resource": RawFilesResource(
            url=f"s3://weave.energy-branches/{os.getenv("DAGSTER_CLOUD_GIT_BRANCH")}/data/raw"
        ),
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


def deployment_name():
    # Manually set env names override anything else
    local_env_name = os.getenv("DAGSTER_DEPLOYMENT")
    if local_env_name is not None:
        return local_env_name

    # branch deployments need a special case
    is_branch_deploy = os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1"
    if is_branch_deploy:
        # Dagster guarantees this will be available but we have to access it defensively
        # above in all situations, so make sure we fail hard if we're about to use it
        # but it isn't for some reason
        # https://docs.dagster.io/dagster-plus/managing-deployments/reserved-environment-variables
        assert (
            "DAGSTER_CLOUD_GIT_BRANCH" in os.environ
        ), "DAGSTER_CLOUD_GIT_BRANCH not set but DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT == 1"
        return "branch"

    # currently only prod, if it's not a branch deploy, is a cloud deployment
    dagster_env_name = os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME")
    if dagster_env_name is not None:
        return dagster_env_name

    # The safest default if nothing is set
    return "dev_local"


defs = Definitions(
    assets=all_assets,
    sensors=[ssen_lv_feeder_files_sensor],
    resources=resources[deployment_name()],
)
