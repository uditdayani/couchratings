"""Player photos from Wikimedia Commons, with the attribution their licences require.

Route: Cricsheet player id -> ESPNcricinfo id (Cricsheet's people register, people.csv)
-> Wikidata item with that ESPNcricinfo id (property P2697) and an image (P18)
-> the Commons file's thumbnail, author and licence.

Every Commons file is freely licensed, but most licences (CC BY, CC BY-SA) require the author
and licence to be shown next to the photo, so they are stored with it. Run monthly or by hand
(notebook 10_player_photos), not daily: photos rarely change.
"""
import html
import re
from typing import Callable, Dict, Iterable, List, Optional

import pandas as pd
import requests
from pyspark.sql import SparkSession

from . import storage
from .config import Tables

PEOPLE_URL = "https://cricsheet.org/register/people.csv"
SPARQL_URL = "https://query.wikidata.org/sparql"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
# Wikimedia asks every client to identify itself with a contact.
USER_AGENT = "Slogonomics/2.2 (https://github.com/uditdayani/slogonomics; cricket ratings dashboard)"
THUMB_WIDTH = 320
BATCH = 50   # titles per Commons API call (the API's limit)

QUERY = """
SELECT ?item ?cricinfo ?image WHERE {
  ?item wdt:P2697 ?cricinfo ;
        wdt:P18 ?image .
}"""


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def wikidata_images(get: Optional[Callable] = None) -> pd.DataFrame:
    """Every Wikidata item with an ESPNcricinfo player id and an image:
    columns cricinfo_id, wikidata_id, file (the Commons file name, without "File:")."""
    get = get or _session().get
    r = get(SPARQL_URL, params={"query": QUERY, "format": "json"}, timeout=120)
    r.raise_for_status()
    rows = []
    for b in r.json()["results"]["bindings"]:
        file = requests.utils.unquote(b["image"]["value"].rsplit("/", 1)[-1])
        rows.append({"cricinfo_id": b["cricinfo"]["value"],
                     "wikidata_id": b["item"]["value"].rsplit("/", 1)[-1],
                     "file": file.replace("_", " ")})
    # One photo per player: the first listed.
    return pd.DataFrame(rows, columns=["cricinfo_id", "wikidata_id", "file"]).drop_duplicates("cricinfo_id")


def _plain(text: Optional[str]) -> Optional[str]:
    """Commons' author field is HTML (often a link): keep the words."""
    if not text:
        return None
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", text))).strip() or None


def commons_metadata(files: Iterable[str], get: Optional[Callable] = None) -> pd.DataFrame:
    """Thumbnail URL, file page, author and licence for Commons files, 50 per request.
    Columns: file, image_url, page_url, artist, license."""
    get = get or _session().get
    files = list(dict.fromkeys(files))
    rows: List[Dict] = []
    for i in range(0, len(files), BATCH):
        titles = "|".join(f"File:{f}" for f in files[i:i + BATCH])
        r = get(COMMONS_API, params={
            "action": "query", "format": "json", "titles": titles, "prop": "imageinfo",
            "iiprop": "url|extmetadata", "iiurlwidth": THUMB_WIDTH,
            "iiextmetadatafilter": "Artist|LicenseShortName"}, timeout=120)
        r.raise_for_status()
        q = r.json().get("query", {})
        # The API may normalise titles; map them back to what was asked for.
        back = {n["to"]: n["from"] for n in q.get("normalized", [])}
        for page in q.get("pages", {}).values():
            info = (page.get("imageinfo") or [None])[0]
            if not info:
                continue
            meta = info.get("extmetadata", {})
            title = back.get(page["title"], page["title"])
            rows.append({"file": title.split(":", 1)[1],
                         "image_url": info.get("thumburl") or info.get("url"),
                         "page_url": info.get("descriptionurl"),
                         "artist": _plain(meta.get("Artist", {}).get("value")),
                         "license": _plain(meta.get("LicenseShortName", {}).get("value"))})
    return pd.DataFrame(rows, columns=["file", "image_url", "page_url", "artist", "license"])


def build_player_photos(spark: SparkSession, t: Tables, people_csv: str,
                        images: Optional[pd.DataFrame] = None,
                        metadata: Optional[Callable[[Iterable[str]], pd.DataFrame]] = None) -> dict:
    """Write silver.player_photos (player_id, image_url, page_url, artist, license,
    wikidata_id) for rated players with a Commons photo. `images` and `metadata` can be
    passed in (tests); by default they come from Wikidata and Commons."""
    people = pd.read_csv(people_csv, dtype=str)[["identifier", "key_cricinfo"]].dropna()
    ours = {r.player_id for r in spark.table(t.silver_players).select("player_id").collect()}
    people = people[people["identifier"].isin(ours)]
    images = wikidata_images() if images is None else images
    linked = people.merge(images, left_on="key_cricinfo", right_on="cricinfo_id")
    meta = (metadata or commons_metadata)(linked["file"])
    photos = (linked.merge(meta, on="file")
              .rename(columns={"identifier": "player_id"})
              [["player_id", "image_url", "page_url", "artist", "license", "wikidata_id"]]
              .dropna(subset=["image_url"]).drop_duplicates("player_id"))
    schema = ("player_id string, image_url string, page_url string, artist string, "
              "license string, wikidata_id string")
    storage.overwrite(spark.createDataFrame(photos.astype(object).where(photos.notna(), None), schema),
                      t.silver_player_photos)
    return {"players": len(ours), "with_cricinfo_id": len(people),
            "on_wikidata_with_image": len(linked), "photos": len(photos)}
