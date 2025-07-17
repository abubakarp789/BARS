"""
Base Scraper Module

Provides a robust, abstract base class for all web scrapers in the project,
ensuring a consistent structure and providing common utility functions.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from dateutil import parser as date_parser

# Import EnhancedNLPExtractor for type hinting
from src.bars.core.nlp_extractor import EnhancedNLPExtractor


class BaseScraper(ABC):
    """
    Abstract base class for all web scrapers.

    This class provides a common interface and shared utilities for all scraper
    subclasses, promoting code reuse and a standardized design pattern.
    """

    def __init__(self, base_url: str, name: str, nlp_extractor: Optional[EnhancedNLPExtractor] = None, **kwargs):
        """
        Initialize the scraper with its core configuration.

        Args:
            base_url: The root URL of the website to be scraped.
            name: The unique name of the scraper (e.g., 'variety', 'kidscreen').
            nlp_extractor: An instance of the NLP extractor class.
            **kwargs: Additional, optional configuration for the scraper.
        """
        self.base_url = base_url.rstrip("/")
        self.name = name
        self.nlp_extractor = nlp_extractor
        # These are good examples of configurable parameters, even if not all
        # child scrapers use them directly.
        self.max_retries = kwargs.get("max_retries", 3)
        self.logger = logging.getLogger(f"scraper.{self.name.lower()}")

    @abstractmethod
    async def scrape(self) -> List[Dict[str, Any]]:
        """
        The main scraping method, which must be implemented by all subclasses.

        This method should contain the primary logic for navigating to a website,
        finding article links, and processing them to extract data.

        Returns:
            A list of dictionaries, where each dictionary represents a scraped article.
        """
        pass

    def _parse_date(self, date_string: str) -> Optional[datetime]:
        """
        Parse a date string in various formats into a timezone-aware datetime object.

        Args:
            date_string: The date string to parse.

        Returns:
            A datetime object or None if parsing fails.
        """
        if not date_string:
            return None
        try:
            # Use dateutil.parser to handle various date formats
            dt = date_parser.parse(date_string)
            # If the datetime object is naive, make it timezone-aware (assuming UTC)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            self.logger.warning(f"Could not parse date: {date_string}")
            return None

    def _clean_text(self, text: str) -> str:
        """
        Cleans and normalizes a string of text by removing excess whitespace.

        Args:
            text: The input string to clean.

        Returns:
            A cleaned string with normalized whitespace.
        """
        if not text:
            return ""
        # Collapse multiple whitespace characters into a single space
        return " ".join(text.split()).strip()

    def _get_full_url(self, path: str) -> str:
        """
        Converts a relative URL path to an absolute URL.

        Args:
            path: The relative path (e.g., '/news/article.html').

        Returns:
            The full, absolute URL.
        """
        if not path:
            return self.base_url

        # If the path is already an absolute URL, return it as is.
        if path.startswith(("http://", "https://")):
            return path

        # Otherwise, join it with the base URL.
        return urljoin(self.base_url, path.lstrip("/"))

    def _is_valid_url(self, url: str) -> bool:
        """
        Checks if a given URL string is well-formed.

        Args:
            url: The URL string to validate.

        Returns:
            True if the URL is valid, False otherwise.
        """
        if not url or not isinstance(url, str):
            return False
        try:
            parsed = urlparse(url)
            # A valid URL must have a scheme (http/https) and a network location (domain).
            return all([parsed.scheme, parsed.netloc])
        except ValueError:
            return False

    def _get_timestamp(self) -> str:
        """
        Gets the current UTC timestamp in ISO 8601 format.

        Returns:
            An ISO-formatted string of the current UTC time.
        """
        # CORRECTED: Use timezone.utc for modern, non-deprecated way to get UTC time.
        return datetime.now(timezone.utc).isoformat()

    async def __aenter__(self):
        """Async context manager entry. Returns the scraper instance."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit. Currently does nothing."""
        pass
