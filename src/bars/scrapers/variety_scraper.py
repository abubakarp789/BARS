"""
Variety Scraper

Scrapes entertainment news from Variety.com with improved resilience against
anti-bot measures, robust error handling, and intelligent content extraction.
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

# Assuming base_scraper is in the same directory or a reachable path
from src.bars.scrapers.base_scraper import BaseScraper

# CSS Selector Constants
ARTICLE_CONTAINER_SELECTOR = "article.l-article-container"
TITLE_SELECTORS = [
    "h1.c-title",
    "h1",
    "title"
]
DATE_SELECTORS = [
    "time",
    "meta[property=\"article:published_time\"]",
    "meta[name=\"date\"]",
    ".article-date",
    ".post-date"
]
CONTENT_CONTAINER_SELECTOR = "div.c-content"
UNWANTED_SELECTORS = [
    "script", "style", "aside", "figure", "form", ".c-ad", ".l-article-sidebar", ".c-related-links"
]
ARTICLE_LINK_SELECTOR = "div.o-tease-list a.c-title__link"


class VarietyScraper(BaseScraper):
    """
    Enhanced scraper for Variety.com.
    Uses playwright-stealth to avoid detection and handles dynamic content.
    """

    def __init__(self, test_mode: bool = False, **kwargs):
        """Initialize the Variety scraper."""
        super().__init__(base_url="https://variety.com", name="variety", **kwargs)
        self.test_mode = test_mode

    async def scrape_article_content(
        self, page: Page, article_url: str
    ) -> Optional[Dict[str, Any]]:
        """
        Scrape the main content of a single article page using page.evaluate.

        Args:
            page: The Playwright page object (pre-configured with stealth).
            article_url: URL of the article to scrape.

        Returns:
            A dictionary containing the article data or None if scraping failed.
        """
        for attempt in range(3):
            try:
                print(f"Scraping article: {article_url} (Attempt {attempt + 1})")
                await page.goto(article_url, wait_until="load", timeout=120000)
                await page.wait_for_load_state("networkidle")

                # Wait for the main article container to be present
                await page.wait_for_selector(
                    ARTICLE_CONTAINER_SELECTOR, timeout=90000
                )

                article_data = await page.evaluate(
                    fr"""() => {{
                    const article = document.querySelector('{ARTICLE_CONTAINER_SELECTOR}');
                    if (!article) return null;

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

                    const contentEl = article.querySelector('{CONTENT_CONTAINER_SELECTOR}');
                    if (!contentEl) return null;
                    const clone = contentEl.cloneNode(true);
                    const unwantedSelectors = {UNWANTED_SELECTORS};
                    unwantedSelectors.forEach(sel => {{
                        clone.querySelectorAll(sel).forEach(unwantedEl => unwantedEl.remove());
                    }});
                    const content = clone.textContent.replace(/\s+/g, ' ').trim();
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
                        "  ✅ Successfully extracted content for: "
                        + article_data.get("title", article_url)
                    )
                    return article_data

                if (
                    not article_data
                    or not article_data.get("title")
                    or article_data.get("title") == "No title found"
                ):
                    print(
                        f"  ⚠️ Could not extract meaningful title from {article_url}. Check selector or page structure."
                    )
                return None

            except Exception as e:
                print(
                    f"  ❌ Error scraping {article_url} (Attempt {attempt + 1}): {str(e)}"
                )
                traceback.print_exc()
                if attempt < 2:
                    await asyncio.sleep(5 * (attempt + 1))
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
        tv_news_url = f"{self.base_url}/v/tv/news/"

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = None
            try:
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
                )
                page = await context.new_page()
                await stealth.apply_stealth_async(page)
                
                # Navigate and get links...
                await page.goto(tv_news_url, wait_until="load", timeout=120000)
                await page.wait_for_load_state("networkidle")
                
                article_links = await page.eval_on_selector_all(
                    ARTICLE_LINK_SELECTOR,
                    "elements => elements.map(el => el.href)",
                )
                await page.close()

                unique_links = list(dict.fromkeys(article_links))
                print(f"Found {len(unique_links)} unique article links to process.")

                links_to_process = unique_links
                if self.test_mode:
                    print(f"[TEST] Limiting to 5 articles out of {len(unique_links)}.")
                    links_to_process = unique_links[:5]

                tasks = [self._scrape_and_process_article(url, context) for url in links_to_process]
                
                # Run all scraping tasks concurrently
                results = await asyncio.gather(*tasks)

                # Flatten the list of lists into a single list of articles
                all_articles = [item for sublist in results for item in sublist]
                return all_articles

            except Exception as e:
                print(f"An error occurred during the scraping process: {e}")
                return []
            finally:
                if context:
                    await context.close()
                await browser.close()


async def scrape_variety(test_mode: bool = False, **kwargs) -> List[Dict]:
    """Helper function to run the Variety scraper."""
    scraper = VarietyScraper(test_mode=test_mode, **kwargs)
    return await scraper.scrape()


if __name__ == "__main__":

    async def main():
        print("🚀 Starting Variety scraper in standalone test mode...")
        scraped_articles = await scrape_variety(test_mode=False)
        print(f"\n✅ Scraped {len(scraped_articles)} articles:")
        for article in scraped_articles:
            print("  - Title: " + article["title"])
            print("    URL: " + article["url"])
            print("    Content Preview: " + article["content"][:200] + "...")

    # To run this file directly for testing: python -m scrapers.variety_scraper
    asyncio.run(main())
