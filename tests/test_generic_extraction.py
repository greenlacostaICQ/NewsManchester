"""Generic article extraction safety net (trafilatura).

Sources without a tuned parser, or whose tuned parser returns too little, must
still yield real article body instead of empty/thin evidence. _trafilatura_evidence
reads the main content from any article page and strips nav/footer chrome.
"""

from __future__ import annotations

import unittest

from news_digest.pipeline.collector.extract import _trafilatura_evidence


ARTICLE_HTML = """
<html><head><title>Some Manchester site</title></head><body>
<nav>Home About Contact Subscribe Newsletter</nav>
<article>
<h1>New tram line opens in Greater Manchester</h1>
<p>A new Metrolink tram line connecting Bury and Manchester city centre opened
on Tuesday after two years of construction work, Transport for Greater
Manchester confirmed.</p>
<p>The line is expected to carry around 40,000 passengers a day and cut journey
times between the two areas by roughly twenty minutes during peak hours.</p>
<p>Local leaders said the extension was part of a wider plan to expand the Bee
Network across the city region by the end of the decade.</p>
</article>
<footer>Cookie policy. Privacy. Terms and conditions.</footer>
</body></html>
"""


class TrafilaturaEvidenceTests(unittest.TestCase):
    def test_extracts_article_body_strips_chrome(self) -> None:
        text = _trafilatura_evidence(ARTICLE_HTML)
        self.assertIn("Metrolink tram line", text)
        self.assertIn("40,000 passengers", text)
        # Precision extraction drops nav/footer boilerplate.
        self.assertNotIn("Cookie policy", text)

    def test_empty_input_returns_empty(self) -> None:
        self.assertEqual(_trafilatura_evidence(""), "")


if __name__ == "__main__":
    unittest.main()
