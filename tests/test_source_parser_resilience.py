from __future__ import annotations

import unittest
from unittest import mock

from news_digest.pipeline.collector.extract import _extract_source_candidates
from news_digest.pipeline.collector.sources import SourceDef


class SourceParserResilienceTest(unittest.TestCase):
    def test_designmynight_subdomain_cards_survive_source_filter(self) -> None:
        source = SourceDef(
            name="DesignMyNight Manchester",
            report_category="culture_weekly",
            candidate_category="culture_weekly",
            url="https://www.designmynight.com/manchester/whats-on/things-to-do-this-weekend-in-manchester",
            primary_block="weekend_activities",
            source_type="html_designmynight",
            allowed_hosts=("designmynight.com",),
            max_candidates=5,
        )
        html = """
        <main>
          <article id="card-cherry-jam" class="card">
            <h3 class="card__title">
              <a href="http://cherry-jam.designmynight.com">Cherry Jam</a>
            </h3>
            <div class="card__description">
              <p>Things to do in Manchester this weekend with Sunday sharing boards and bottomless cocktails.</p>
            </div>
          </article>
        </main>
        """

        [candidate] = _extract_source_candidates(source, html)

        self.assertEqual(candidate["title"], "Cherry Jam")
        self.assertIn("cherry-jam.designmynight.com", candidate["source_url"])
        self.assertEqual(candidate["primary_block"], "weekend_activities")

    @mock.patch("news_digest.pipeline.collector.extract._fetch_text")
    def test_bdaily_listing_fetches_child_articles_before_gm_filter(self, fetch_text: mock.Mock) -> None:
        source = SourceDef(
            name="Bdaily Manchester",
            report_category="tech_business",
            candidate_category="tech_business",
            url="https://bdaily.co.uk/region/north-west",
            primary_block="tech_business",
            allowed_hosts=("bdaily.co.uk",),
            max_candidates=5,
        )
        listing = """
        <main>
          <h2><a href="https://bdaily.co.uk/articles/2026/06/21/logros-expands-corporate-finance-team-with-partner-hire">
            Logros expands corporate finance team with partner hire
          </a></h2>
        </main>
        """
        article = """
        <html>
          <head>
            <script type="application/ld+json">
              {"datePublished": "2026-06-21T22:11:13+00:00"}
            </script>
            <meta name="description" content="Corporate finance advisory firm Logros Advisory Partners, headquartered in Manchester, has appointed William Senior.">
          </head>
          <body>
            <h1>Logros expands corporate finance team with partner hire</h1>
            <p>Corporate finance advisory firm Logros Advisory Partners, headquartered in Manchester, has appointed William Senior as corporate finance partner.</p>
          </body>
        </html>
        """
        fetch_text.return_value = article

        [candidate] = _extract_source_candidates(source, listing)

        self.assertEqual(candidate["title"], "Logros expands corporate finance team with partner hire")
        self.assertEqual(candidate["primary_block"], "tech_business")
        self.assertIn("Manchester", candidate["summary"])
        self.assertIn("Manchester", candidate["evidence_text"])

    @mock.patch("news_digest.pipeline.collector.extract._fetch_text")
    def test_bbc_sport_team_rss_falls_back_to_html_article_surface(self, fetch_text: mock.Mock) -> None:
        source = SourceDef(
            name="BBC Sport Manchester United",
            report_category="football",
            candidate_category="football",
            url="https://feeds.bbci.co.uk/sport/football/teams/manchester-united/rss.xml",
            primary_block="football",
            source_type="rss",
            allowed_hosts=("feeds.bbci.co.uk", "bbc.co.uk", "bbc.com"),
            max_candidates=8,
        )
        rss = """
        <rss><channel>
          <link>https://www.bbc.co.uk/sport/football/teams/manchester-united</link>
          <item><title>Manchester United</title><link>https://www.bbc.co.uk/sounds/play/p0test</link></item>
        </channel></rss>
        """
        team_page = """
        <main>
          <a href="/sport/football/articles/ckg02v1yvx1o">Man Utd will not enter EFL Trophy next season</a>
        </main>
        """
        article = """
        <html><head>
          <script type="application/ld+json">{"datePublished": "2026-06-23T11:37:43+00:00"}</script>
          <meta name="description" content="Manchester United have decided not to enter the EFL Trophy next season.">
        </head><body>
          <p>Manchester United have decided not to enter the EFL Trophy or National League Cup for 2026-27.</p>
        </body></html>
        """
        fetch_text.side_effect = lambda url, *args, **kwargs: team_page if "teams/manchester-united" in url else article

        [candidate] = _extract_source_candidates(source, rss)

        self.assertEqual(candidate["title"], "Man Utd will not enter EFL Trophy next season")
        self.assertEqual(candidate["primary_block"], "football")
        self.assertIn("/sport/football/articles/", candidate["source_url"])

    @mock.patch("news_digest.pipeline.collector.extract._fetch_text")
    def test_mancity_enrichment_preserves_listing_card_title(self, fetch_text: mock.Mock) -> None:
        source = SourceDef(
            name="Manchester City",
            report_category="football",
            candidate_category="football",
            url="https://www.mancity.com/news?tag=News",
            primary_block="football",
            allowed_hosts=("mancity.com",),
            max_candidates=5,
        )
        listing = """
        <main>
          <a href="/news/mens/man-city-record-fee-123456">
            Man City agree record fee with Forest for Anderson
          </a>
        </main>
        """
        article = """
        <html><head>
          <title>Man City Men's Team News - Manchester City F.C.</title>
          <script type="application/ld+json">{"datePublished": "2026-06-24T08:00:00+00:00"}</script>
          <meta name="description" content="Manchester City have agreed a record fee with Nottingham Forest.">
        </head><body>
          <p>Manchester City have agreed a record fee with Nottingham Forest for Anderson.</p>
        </body></html>
        """
        fetch_text.return_value = article

        [candidate] = _extract_source_candidates(source, listing)

        self.assertEqual(candidate["title"], "Man City agree record fee with Forest for Anderson")
        self.assertNotEqual(candidate["title"], "Man City Men's Team News - Manchester City F.C.")
        self.assertEqual(candidate["primary_block"], "football")


if __name__ == "__main__":
    unittest.main()
