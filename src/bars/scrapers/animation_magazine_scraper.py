"""
Animation Magazine Scraper

Scrapes animation industry news and articles from Animation Magazine with improved
resilience against anti-bot measures and more robust error handling.
"""

import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import traceback
import sys
import os

# Add the project root to the sys.path for absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Core scraping libraries
from playwright.async_api import async_playwright, Page, Browser
from playwright_stealth.stealth import Stealth

stealth = Stealth()

# CSS Selector Constants
ARTICLE_CONTAINER_SELECTORS = ["div.entry-content", "div.td-post-content"]
TITLE_SELECTORS = [
    "h1.entry-title", "h1.td-post-title", "h1.article-title", "h1.post-title", "h1", "title"
]
DATE_SELECTORS = [
    "time.entry-date", "time.published", "time.updated", ".entry-date", ".post-date", ".article-date", "meta[property=\"article:published_time\"]", "meta[property=\"og:published_time\"]", "meta[name=\"date\"]"
]
CONTENT_SELECTORS = [
    "div.td-post-content", "div.entry-content", "article", "div.post-content", "div.article-content", "div.content", "main", "div#content", "div#main"
]
UNWANTED_SELECTORS = [
    "script", "style", "nav", "footer", "header", "form", "img", "figure", "figcaption", "iframe", ".social-share", ".related-posts", ".comments", ".ad-container", ".ad", ".advertisement", ".newsletter", ".subscribe", ".author-box"
]

# Assuming base_scraper is in the same directory or a reachable path
from src.bars.scrapers.base_scraper import BaseScraper


class AnimationMagazineScraper(BaseScraper):
    """
    Enhanced scraper for Animation Magazine website.
    Uses playwright-stealth to avoid detection.
    """

    def __init__(self, test_mode: bool = False, **kwargs):
        """Initialize the Animation Magazine scraper."""
        super().__init__(
            base_url="https://www.animationmagazine.net",
            name="animation_magazine",
            **kwargs,
        )
        self.test_mode = test_mode

    async def scrape_article_content(
        self, page: Page, article_url: str
    ) -> Optional[Dict[str, Any]]:
        """
        Scrape the main content of a single article page.

        Args:
            page: The Playwright page object (pre-configured with stealth).
            article_url: URL of the article to scrape.

        Returns:
            A dictionary containing the article data or None if scraping failed.
        """
        for attempt in range(3):
            try:
                print(f"Scraping article: {article_url} (Attempt {attempt + 1})")
                # Use a longer timeout and wait for DOM content to be loaded.
                await page.goto(
                    article_url, wait_until="domcontentloaded", timeout=90000
                )

                # Wait for a specific element that indicates the article body is present.
                await page.wait_for_selector(
                    ARTICLE_CONTAINER_SELECTORS[0] + ", " + ARTICLE_CONTAINER_SELECTORS[1], timeout=60000
                )

                # This JavaScript is well-written for extracting content from multiple possible layouts.
                # The r"" string notation prevents Python's SyntaxWarning.
                article_data = await page.evaluate(
                    f"""() => {{
                    // Try multiple selectors for title
                    let title = '';
                    const titleSelectors = {TITLE_SELECTORS};
                    for (const selector of titleSelectors) {{
                        const el = document.querySelector(selector);
                        if (el) {{
                            title = el.textContent.trim();
                            if (title) break;
                        }}
                    }}
                    if (!title && document.title) title = document.title;

                    // Try multiple selectors for date
                    let date = '';
                    const dateSelectors = {DATE_SELECTORS};
                    for (const selector of dateSelectors) {{
                        const el = document.querySelector(selector);
                        if (el) {{
                            date = el.getAttribute('datetime') || el.getAttribute('content') || el.textContent.trim();
                            if (date) break;
                        }}
                    }}
                    if (!date) date = new Date().toISOString();

                    // Get the main content, cleaning out unwanted elements
                    const contentSelectors = {CONTENT_SELECTORS};
                    let content = '';
                    for (const selector of contentSelectors) {{
                        const el = document.querySelector(selector);
                        if (el) {{
                            const clone = el.cloneNode(true);
                            const unwantedSelectors = {UNWANTED_SELECTORS};
                            unwantedSelectors.forEach(sel => {{
                                clone.querySelectorAll(sel).forEach(unwantedEl => unwantedEl.remove());
                            }});
                            content = clone.textContent.replace(/\s+/g, ' ').trim();
                            if (content.length > 200) {{
                                break;
                            }}
                        }}
                    }}
                    return {{
                        title: title || 'No title found',
                        content: content,
                        date: date || new Date().toISOString(),
                        url: window.location.href
                    }};
                }}"""
                )

                if article_data and article_data.get("content"):
                    print(
                        f"  ✅ Successfully extracted content for: {article_data.get('title', article_url)}"
                    )
                    return article_data

                print(
                    f"  ⚠️ Could not extract meaningful content from {article_url}. Skipping."
                )
                return None

            except Exception as e:
                print(
                    f"  ❌ Error scraping {article_url} (Attempt {attempt + 1}): {str(e)}"
                )
                if attempt < 2:
                    await asyncio.sleep(3 * (attempt + 1))  # Exponential backoff
                else:
                    print(f"  Failed to scrape article after 3 attempts: {article_url}")
                    return None
        return None

    async def _scrape_and_process_article(self, url: str, context) -> List[Dict]:
        """
        Scrapes and processes a single article page concurrently.

        Args:
            url: The URL of the article to process.
            context: The browser context to use for creating a new page.

        Returns:
            A list of deal records extracted from the article.
        """
        if not url or not isinstance(url, str):
            return []

        page = await context.new_page()
        processed_records = []
        try:
            article_data = await self.scrape_article_content(page, url)

            if article_data and article_data.get("content"):
                nlp_data = self.nlp_extractor.extract_deal_info(
                    article_data.get("content", ""), article_data.get("date")
                )

                for deal in nlp_data.get("deals", []):
                    record = {
                        "source": self.name,
                        "url": article_data.get("url"),
                        "title": article_data.get("title"),
                        "published_at": self._parse_date(article_data.get("date")),
                        "content": article_data.get("content"),
                        "broadcaster_name": deal.get("broadcaster"),
                        "show_title": deal.get("show"),
                        "deal_type": deal.get("deal_type", "other"),
                        "genres": deal.get("genres", []),
                        "regions": deal.get("regions", []),
                        "created_at": datetime.now(timezone.utc),
                    }
                    processed_records.append(record)
        except Exception as e:
            print(f"  ❌ Critical error processing article {url}. Skipping. Error: {e}")
        finally:
            await page.close()

        return processed_records

    async def scrape(self) -> List[Dict]:
        """
        Main scraping method. Launches a browser, finds article links,
        and then scrapes them concurrently.
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = None
            try:
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
                )
                page = await context.new_page()
                await stealth.apply_stealth_async(page)
                
                await page.goto(
                    self.base_url, wait_until="domcontentloaded", timeout=90000
                )
                await page.wait_for_selector(
                    "div.td-main-content-wrap", timeout=60000
                )
                await asyncio.sleep(2)

                article_links = await page.eval_on_selector_all(
                    "h3.entry-title a", "elements => elements.map(el => el.href)"
                )
                await page.close()

                unique_links = list(dict.fromkeys(article_links))
                print(f"Found {len(unique_links)} unique article links to process.")

                links_to_process = unique_links
                if self.test_mode:
                    print(f"[TEST] Limiting to 5 articles out of {len(unique_links)}.")
                    links_to_process = unique_links[:5]

                tasks = [self._scrape_and_process_article(url, context) for url in links_to_process]
                results = await asyncio.gather(*tasks)

                all_articles = [item for sublist in results for item in sublist]
                return all_articles
                
            except Exception as e:
                print(f"An error occurred during the scraping process: {e}")
                return []
            finally:
                if context:
                    await context.close()
                await browser.close()


async def scrape_animation_magazine(test_mode: bool = False, **kwargs) -> List[Dict]:
    """Helper function to run the Animation Magazine scraper."""
    scraper = AnimationMagazineScraper(test_mode=test_mode, **kwargs)
    return await scraper.scrape()


if __name__ == "__main__":

    async def main():
        print("🚀 Starting Animation Magazine scraper in standalone test mode...")
        # To run a full scrape, set test_mode=False
        scraped_articles = await scrape_animation_magazine(test_mode=False)
        print(f"\n✅ Scraped {len(scraped_articles)} articles:")
        for article in scraped_articles:
            print(f"  - Title: {article['title']}")
            print(f"    URL: {article['url']}")
            print(f"    Content Length: {len(article['content'])} chars")

    # To run this file directly for testing: python -m scrapers.animation_magazine_scraper
    asyncio.run(main())
