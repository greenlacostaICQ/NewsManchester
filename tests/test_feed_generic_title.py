"""Generic-title RSS recovery.

Some live feeds (notably BBC Sport team feeds) repeat the section/team name as
the <title> on every item and put the actual story in <description>. Reading
<title> alone makes every item identical, so dedup collapses the whole feed to
nothing. The parser must derive distinct headlines from the description.
"""

from __future__ import annotations

import unittest

from news_digest.pipeline.collector.extract import _extract_feed_items


GENERIC_FEED = """<?xml version="1.0"?>
<rss version="2.0"><channel>
<title>Man Utd - BBC Sport</title>
<item><title>Manchester United</title>
<link>https://www.bbc.co.uk/sport/football/articles/aaa</link>
<description>United have completed the £40m signing of a new striker for the summer window.</description></item>
<item><title>Manchester United</title>
<link>https://www.bbc.co.uk/sport/football/articles/bbb</link>
<description>Marcus Rashford has been ruled out for three weeks with a hamstring injury.</description></item>
</channel></rss>"""


NORMAL_FEED = """<?xml version="1.0"?>
<rss version="2.0"><channel>
<title>Manchester Evening News</title>
<item><title>Greater Manchester unveils new cycling network across the region</title>
<link>https://www.manchestereveningnews.co.uk/news/aaa</link>
<description>Details were announced on Tuesday by local leaders.</description></item>
</channel></rss>"""


class FeedGenericTitleTests(unittest.TestCase):
    def test_generic_titles_recovered_from_description(self) -> None:
        items = _extract_feed_items("https://www.bbc.co.uk", GENERIC_FEED)
        self.assertEqual(len(items), 2)
        titles = [item.title for item in items]
        # No item keeps the generic team name as its headline.
        self.assertTrue(all(title != "Manchester United" for title in titles))
        # Headlines are distinct (no dedup collapse) and come from the description.
        self.assertEqual(len(set(titles)), 2)
        self.assertTrue(any("striker" in title for title in titles))
        self.assertTrue(any("Rashford" in title for title in titles))

    def test_descriptive_titles_preserved(self) -> None:
        items = _extract_feed_items("https://www.manchestereveningnews.co.uk", NORMAL_FEED)
        self.assertEqual(len(items), 1)
        self.assertEqual(
            items[0].title,
            "Greater Manchester unveils new cycling network across the region",
        )


if __name__ == "__main__":
    unittest.main()
