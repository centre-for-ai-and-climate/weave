from dagster import Definitions, load_assets_from_modules

from .assets import dno_smart_meter_files

all_assets = load_assets_from_modules([dno_smart_meter_files])

defs = Definitions(
    assets=all_assets,
)
