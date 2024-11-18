import os

from dagster import build_asset_context

from weave.assets.ons import onspd
from weave.resources.ons import StubONSAPIClient
from weave.resources.output_files import OutputFilesResource

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "..",
    "fixtures",
)


def test_onspd(tmp_path):
    output_dir = tmp_path / "ons"
    output_dir.mkdir()

    context = build_asset_context()
    raw_files_resource = OutputFilesResource(url=tmp_path.as_uri())
    ons_api_client = StubONSAPIClient(
        file_to_download=os.path.join(FIXTURE_DIR, "onspd.zip"),
    )
    onspd(context, ons_api_client, raw_files_resource)

    with open(output_dir / "onspd.zip", "rb") as f:
        df = ons_api_client.onspd_dataframe(f)
        assert len(df) == 9
