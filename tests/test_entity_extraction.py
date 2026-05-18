from __future__ import annotations

import unittest

from news_digest.pipeline.entity_extraction import (
    ENTITY_SCHEMA_VERSION,
    enrich_candidate_entities,
    extract_entities,
)


class EntityExtractionTest(unittest.TestCase):
    def test_extracts_borough_council_station_and_company(self) -> None:
        candidate = {
            "title": "Manchester City Council confirms works near Manchester Piccadilly",
            "summary": "TfGM says passengers should check services around Piccadilly station.",
            "source_label": "Manchester Council",
        }

        entities = extract_entities(candidate)

        self.assertEqual(entities["schema_version"], ENTITY_SCHEMA_VERSION)
        self.assertIn("Manchester", entities["boroughs"])
        self.assertIn("Manchester City Council", entities["councils"])
        self.assertIn("Manchester Piccadilly", entities["stations"])
        self.assertIn("TfGM", entities["companies"])
        self.assertTrue(any(e["type"] == "station" for e in entities["all"]))

    def test_extracts_venue_district_and_club(self) -> None:
        candidate = {
            "title": "Manchester United event at The Deaf Institute in Northern Quarter",
            "summary": "The evening takes place near Manchester city centre.",
            "category": "venues_tickets",
        }

        entities = extract_entities(candidate)

        self.assertIn("Manchester United", entities["clubs"])
        self.assertIn("The Deaf Institute", entities["venues"])
        self.assertIn("Northern Quarter", entities["districts"])
        self.assertIn("City Centre", entities["districts"])

    def test_extracts_company_suffixes(self) -> None:
        candidate = {
            "title": "Acme Developments submits Stockport plan",
            "summary": "The company says the scheme is near Stockport station.",
        }

        entities = extract_entities(candidate)

        self.assertIn("Stockport", entities["boroughs"])
        self.assertIn("Stockport", entities["stations"])
        self.assertIn("Acme Developments", entities["companies"])

    def test_enrichment_sets_top_level_borough_for_compatibility(self) -> None:
        candidate = {"title": "Oldham Council opens new market"}

        enriched = enrich_candidate_entities(candidate)

        self.assertEqual(enriched["borough"], "Oldham")
        self.assertIn("Oldham Council", enriched["entities"]["councils"])

    def test_avoids_common_manchester_false_positives(self) -> None:
        entities = extract_entities(
            {
                "title": "Pedestrian injured in Manchester city centre",
                "summary": "News Greater Manchester News Manchester City Centre",
            }
        )

        self.assertIn("City Centre", entities["districts"])
        self.assertNotIn("Manchester City", entities["clubs"])


if __name__ == "__main__":
    unittest.main()
