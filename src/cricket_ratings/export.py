"""Export the dashboard's datasets as Parquet files and publish them to a GitHub release.

The dashboard can then read plain files from a public URL instead of querying the SQL
warehouse: no Databricks token in the app, no warehouse to wake up, and the app keeps
working even if the workspace is paused. The queries are the dashboard's own
(app/queries.py), so both routes show exactly the same data.

Files go to one release (default tag `data-latest`), replaced in place each run: a new
copy is uploaded under a temporary name first, then swapped in, so the app never finds a
file missing for more than a moment.
"""
import os
from typing import Dict, Iterable, Optional

import requests
from pyspark.sql import SparkSession

API = "https://api.github.com"
UPLOADS = "https://uploads.github.com"


def export_datasets(spark: SparkSession, queries_module, catalog: str, version: str, out_dir: str,
                    names: Optional[Iterable[str]] = None) -> Dict[str, int]:
    """Run each dataset's query and write <out_dir>/<name>.parquet. Returns rows per file.
    Optional datasets whose tables don't exist yet are skipped."""
    q = queries_module
    os.makedirs(out_dir, exist_ok=True)
    sql = dict(q.queries(catalog))
    sql["balls"] = q.balls_query(catalog, one_match=False)
    names = list(names or (tuple(q.CORE) + tuple(q.EXTRA)))
    written = {}
    for name in names:
        try:
            df = spark.sql(sql[name], args={"version": version})
        except Exception:
            if name == "matches":            # before the venue register exists
                df = spark.sql(q.matches_query(catalog, venue_register=False), args={"version": version})
            elif name in q.CORE and name != "validation":
                raise
            else:
                continue
        pdf = df.toPandas()
        # Spark Connect (Databricks serverless) puts query metrics in .attrs, which pandas
        # would try to write into the Parquet metadata as JSON and fail.
        pdf.attrs = {}
        pdf.to_parquet(os.path.join(out_dir, f"{name}.parquet"), index=False)
        written[name] = len(pdf)
    return written


class GitHubRelease:
    """Just enough of the GitHub releases API to keep one release's files up to date."""

    def __init__(self, repo: str, token: str, session: Optional[requests.Session] = None):
        self.repo = repo
        self.s = session or requests.Session()
        self.s.headers.update({"Authorization": f"Bearer {token}",
                               "Accept": "application/vnd.github+json",
                               "X-GitHub-Api-Version": "2022-11-28"})

    def _ok(self, r):
        if r.status_code >= 400:
            raise RuntimeError(f"GitHub API {r.request.method} {r.url}: {r.status_code} {r.text[:300]}")
        return r

    def release(self, tag: str) -> dict:
        """The release for `tag`, created (not marked as the latest release) if it doesn't exist."""
        r = self.s.get(f"{API}/repos/{self.repo}/releases/tags/{tag}")
        if r.status_code == 404:
            r = self.s.post(f"{API}/repos/{self.repo}/releases", json={
                "tag_name": tag, "name": "Dashboard data (updated daily)", "make_latest": "false",
                "body": "Parquet files the Slogonomics dashboard reads. Replaced by the daily "
                        "Databricks job (notebook 09_export_parquet). Data: Cricsheet, ODC-BY."})
        return self._ok(r).json()

    def replace_asset(self, release: dict, path: str) -> None:
        name = os.path.basename(path)
        assets = {a["name"]: a for a in self._ok(
            self.s.get(f"{API}/repos/{self.repo}/releases/{release['id']}/assets",
                       params={"per_page": 100})).json()}
        tmp = f"new-{name}"
        if tmp in assets:                    # left over from an interrupted run
            self._ok(self.s.delete(f"{API}/repos/{self.repo}/releases/assets/{assets[tmp]['id']}"))
        with open(path, "rb") as f:
            new = self._ok(self.s.post(
                f"{UPLOADS}/repos/{self.repo}/releases/{release['id']}/assets",
                params={"name": tmp}, data=f,
                headers={"Content-Type": "application/octet-stream"})).json()
        if name in assets:
            self._ok(self.s.delete(f"{API}/repos/{self.repo}/releases/assets/{assets[name]['id']}"))
        self._ok(self.s.patch(f"{API}/repos/{self.repo}/releases/assets/{new['id']}", json={"name": name}))


def publish(out_dir: str, repo: str, token: str, tag: str = "data-latest",
            session: Optional[requests.Session] = None) -> list:
    """Upload every .parquet file in `out_dir` to the release, replacing older copies."""
    gh = GitHubRelease(repo, token, session)
    rel = gh.release(tag)
    done = []
    for name in sorted(os.listdir(out_dir)):
        if name.endswith(".parquet"):
            gh.replace_asset(rel, os.path.join(out_dir, name))
            done.append(name)
    return done


def download_url(repo: str, tag: str = "data-latest") -> str:
    """What to put in the app's secrets as `[data] url`."""
    return f"https://github.com/{repo}/releases/download/{tag}"
