"""Player photos: Wikidata and Commons responses parsed, joined and attributed."""
import pandas as pd

from cricket_ratings import photos
from test_pipeline import built  # noqa: F401


class Resp:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


def test_wikidata_images_parsed():
    payload = {"results": {"bindings": [
        {"item": {"value": "http://www.wikidata.org/entity/Q123"}, "cricinfo": {"value": "253802"},
         "image": {"value": "http://commons.wikimedia.org/wiki/Special:FilePath/Virat%20Kohli%20in%202018.jpg"}},
        {"item": {"value": "http://www.wikidata.org/entity/Q123"}, "cricinfo": {"value": "253802"},
         "image": {"value": "http://commons.wikimedia.org/wiki/Special:FilePath/Second.jpg"}}]}}
    df = photos.wikidata_images(get=lambda *a, **k: Resp(payload))
    assert df.to_dict("records") == [{"cricinfo_id": "253802", "wikidata_id": "Q123",
                                      "file": "Virat Kohli in 2018.jpg"}]   # one per player


def test_commons_metadata_parsed_and_batched():
    calls = []

    def get(url, params, timeout):
        titles = params["titles"].split("|")
        calls.append(len(titles))
        pages = {str(i): {"title": t.replace("_", " "), "imageinfo": [{
            "thumburl": f"https://upload/thumb/{i}.jpg", "url": f"https://upload/{i}.jpg",
            "descriptionurl": f"https://commons/{i}",
            "extmetadata": {"Artist": {"value": '<a href="//x">Jane&nbsp;Doe</a>'},
                            "LicenseShortName": {"value": "CC BY-SA 4.0"}}}]}
                 for i, t in enumerate(titles)}
        return Resp({"query": {"pages": pages, "normalized": [
            {"from": t, "to": t.replace("_", " ")} for t in titles if "_" in t]}})

    files = [f"Player_{i}.jpg" for i in range(60)]
    df = photos.commons_metadata(files, get=get)
    assert calls == [50, 10]
    assert len(df) == 60 and set(df["file"]) == set(files)               # names mapped back
    row = df.iloc[0]
    assert row["artist"] == "Jane Doe" and row["license"] == "CC BY-SA 4.0"
    assert row["image_url"].startswith("https://upload/thumb/")


def test_build_player_photos(spark, built, tmp_path):  # noqa: F811
    t, _, _ = built
    ids = [r.player_id for r in spark.table(t.silver_players).select("player_id").collect()]
    people = pd.DataFrame({"identifier": ids[:2] + ["not_ours"], "name": ["a", "b", "c"],
                           "key_cricinfo": ["1", "2", "3"]})
    people.to_csv(tmp_path / "people.csv", index=False)
    images = pd.DataFrame({"cricinfo_id": ["1", "3"], "wikidata_id": ["Q1", "Q3"],
                           "file": ["One.jpg", "Three.jpg"]})

    def meta(files):
        return pd.DataFrame({"file": list(files), "image_url": [f"https://x/{f}" for f in files],
                             "page_url": "https://commons", "artist": "Jane Doe", "license": "CC BY 2.0"})

    summary = photos.build_player_photos(spark, t, str(tmp_path / "people.csv"), images=images, metadata=meta)
    assert summary["photos"] == 1 and summary["with_cricinfo_id"] == 2
    row = spark.table(t.silver_player_photos).first()
    assert (row.player_id, row.image_url, row.license) == (ids[0], "https://x/One.jpg", "CC BY 2.0")
