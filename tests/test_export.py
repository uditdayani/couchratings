"""Parquet export: the dashboard's own queries run in Spark, and the GitHub release swap."""
import os
import sys

import pandas as pd

from cricket_ratings import export, gold_model, gold_ratings, gold_scale
from test_pipeline import built  # noqa: F401

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
import queries  # noqa: E402


def test_export_runs_the_dashboard_queries(spark, built, tmp_path):  # noqa: F811
    t, _, _ = built
    gold_model.fit_model(spark, t, "v1", min_samples=1)
    gold_ratings.run_ratings(spark, t, "v1")
    gold_scale.run_scale(spark, t, "v1")
    written = export.export_datasets(spark, queries, "spark_catalog", "v1", str(tmp_path))
    # validation and photos don't exist in the fixture: skipped, the rest written
    assert {"ratings", "matches", "form", "meta", "matchups", "balls"} <= set(written)
    ratings = pd.read_parquet(tmp_path / "ratings.parquet")
    assert len(ratings) == written["ratings"] and "name" in ratings.columns
    matches = pd.read_parquet(tmp_path / "matches.parquet")
    assert {"venue_id", "venue_name", "event_stage"} <= set(matches.columns)   # fallback query
    balls = pd.read_parquet(tmp_path / "balls.parquet")
    assert {"expected_before", "expected_after", "runs_added", "wicket_kind"} <= set(balls.columns)


class FakeResp:
    def __init__(self, status, payload=None):
        self.status_code, self.payload = status, payload or {}
        self.text, self.url, self.request = "", "", type("R", (), {"method": "X"})()

    def json(self):
        return self.payload


class FakeGitHub:
    """Records calls; one release with an old ratings.parquet already attached."""
    def __init__(self, has_release=True):
        self.headers, self.calls, self.has_release = {}, [], has_release

    def get(self, url, params=None):
        self.calls.append(("GET", url))
        if url.endswith("/releases/tags/data-latest"):
            return FakeResp(200, {"id": 7}) if self.has_release else FakeResp(404)
        return FakeResp(200, [{"name": "ratings.parquet", "id": 100}])

    def post(self, url, json=None, params=None, data=None, headers=None):
        self.calls.append(("POST", url, (params or {}).get("name") or json.get("tag_name")))
        return FakeResp(201, {"id": 7} if json else {"id": 200})

    def delete(self, url):
        self.calls.append(("DELETE", url))
        return FakeResp(204)

    def patch(self, url, json=None):
        self.calls.append(("PATCH", url, json["name"]))
        return FakeResp(200)


def test_publish_swaps_files_in_place(tmp_path):
    (tmp_path / "ratings.parquet").write_bytes(b"new")
    (tmp_path / "notes.txt").write_text("not uploaded")
    gh = FakeGitHub()
    assert export.publish(str(tmp_path), "me/repo", "token", session=gh) == ["ratings.parquet"]
    kinds = [c[0] for c in gh.calls]
    # upload under a temporary name, then delete the old file, then rename the new one
    assert kinds.index("POST") < kinds.index("DELETE") < kinds.index("PATCH")
    assert ("POST", "https://uploads.github.com/repos/me/repo/releases/7/assets", "new-ratings.parquet") in gh.calls
    assert ("DELETE", "https://api.github.com/repos/me/repo/releases/assets/100") in gh.calls
    assert ("PATCH", "https://api.github.com/repos/me/repo/releases/assets/200", "ratings.parquet") in gh.calls
    assert gh.headers["Authorization"] == "Bearer token"


def test_publish_creates_the_release_once(tmp_path):
    (tmp_path / "form.parquet").write_bytes(b"x")
    gh = FakeGitHub(has_release=False)
    export.publish(str(tmp_path), "me/repo", "token", session=gh)
    assert ("POST", "https://api.github.com/repos/me/repo/releases", "data-latest") in gh.calls
    assert export.download_url("me/repo") == "https://github.com/me/repo/releases/download/data-latest"
