"""
Enhanced NLP Extractor

This module processes scraped articles stored in MongoDB, using a combination of
spaCy's named entity recognition and custom rule-based logic to extract structured
deal information.
"""

import spacy
import json
import re
import asyncio
from typing import Dict, List, Any, Optional
from spacy.matcher import PhraseMatcher
from src.bars.core.mongodb_manager import MongoDBManager
from datetime import datetime, timezone
from dateutil.parser import isoparse, parse as dateutil_parse
import os
import sys
from dotenv import load_dotenv

# Add the project root to the sys.path for absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))


class EnhancedNLPExtractor:
    """
    Extracts structured data (deals, broadcasters, shows) from raw article text.
    """

    def __init__(self):
        """Initialize the NLP extractor with spaCy model and MongoDB connection."""
        print("Loading spaCy model...")
        # Using a larger model can yield better NER results, but is slower.
        # For production, consider "en_core_web_trf" or a custom-trained model.
        self.nlp = spacy.load("en_core_web_sm")
        print("spaCy model loaded.")

        self.db_manager = MongoDBManager()

        # A curated list of known broadcasters and production companies.
        # This list is key to improving entity recognition accuracy.
        self.known_broadcasters = [
            "Netflix",
            "Disney+",
            "Hulu",
            "Amazon Prime Video",
            "Apple TV+",
            "HBO Max",
            "Peacock",
            "Paramount+",
            "Crunchyroll",
            "Nickelodeon",
            "Cartoon Network",
            "Warner Bros.",
            "Sony Pictures Animation",
            "DreamWorks Animation",
            "Pixar",
            "Studio Ghibli",
            "BBC",
            "ITV",
            "Channel 4",
            "Sky",
            "ZDF",
            "France Télévisions",
            "CBC",
            "ABC",
            "NBC",
            "CBS",
            "FOX",
            "Adult Swim",
            "Cartoon Brew",
            "Wyncor",
            "Thunderbird Entertainment",
            "Atomic Cartoons",
            "Sphere Media",
            "Viva Kids",
            "Oni Press",
            "Spin Master Entertainment",
            "Pinkfong Company",
            "TBS",
            "Shout! Studios",
            "GKIDS",
            "Anime Limited",
            "Paramount Animation",
            "Nickelodeon Movies",
            "Oware",
            "Blue Zoo Animation",
            "Tat Productions",
            "Brazen Animation",
            "Republic Records",
            "Epic Games",
            "Riot Games",
            "Blizzard",
            "Wacom",
            "Yellowbrick",
            "Imageworks",
            "Cartoon Forum",
            "EBU",
            "Sunrise studio",
            "Alcon Entertainment",
            "DNEG",
            "Prime Focus Studios",
        ]

        # Generic terms to help filter out non-specific organizations.
        self.non_broadcaster_orgs = [
            "inc",
            "llc",
            "corp",
            "group",
            "company",
            "studio",
            "media",
            "entertainment",
            "productions",
            "animation",
            "magazine",
            "film festival",
            "university",
            "school",
            "institute",
            "association",
            "council",
            "foundation",
            "agency",
            "press",
            "records",
            "games",
            "toy co",
            "vfx",
            "music",
            "bank",
            "fund",
            "capital",
            "venture",
            "solutions",
            "consulting",
            "advisory",
            "marketing",
            "pr",
            "public relations",
            "communications",
            "management",
            "talent",
            "artists",
            "creatives",
            "writers",
            "directors",
            "producers",
            "distributors",
            "sales",
            "licensing",
            "consumer products",
            "publishing",
            "books",
            "comics",
            "merchandise",
            "apparel",
            "digital",
            "online",
            "platform",
        ]

        # Keywords to identify the type of deal mentioned in the text.
        self.deal_keywords = {
            "acquisition": [
                "acquisition",
                "acquires",
                "acquired",
                "inks deal with",
                "buys",
                "purchases",
                "scoops up",
            ],
            "licensing": [
                "licensing",
                "licenses",
                "licensed",
                "distributes",
                "distribution deal",
                "rights deal",
            ],
            "co-production": [
                "co-production",
                "co-produces",
                "co-produced",
                "partners with",
                "teams up with",
            ],
            "commission": [
                "commission",
                "commissions",
                "commissioned",
                "greenlights",
                "orders",
            ],
            "development": ["development deal", "in development", "developing"],
            "renewal": ["renewed", "renewal", "second season", "third season"],
        }

        self.genre_keywords = {
            "preschool": ["preschool", "pre-school", "toddler", "kindergarten"],
            "tween": ["tween", "pre-teen", "young adult"],
            "kids": ["kids", "children", "youth"],
            "family": ["family", "all ages"],
            "animation": ["animation", "animated", "cartoon"],
            "live-action": ["live-action"],
            "documentary": ["documentary", "docuseries"],
            "comedy": ["comedy", "sitcom"],
            "drama": ["drama"],
            "action": ["action", "adventure"],
            "sci-fi": ["sci-fi", "science fiction"],
            "fantasy": ["fantasy"],
            "horror": ["horror"],
            "thriller": ["thriller"],
            "anime": ["anime"],
            "stop-motion": ["stop-motion"],
            "cgi": ["cgi", "computer generated imagery", "3d animation"],
            "2d": ["2d animation", "traditional animation"],
            "hybrid": ["hybrid animation"],
        }

        self.region_keywords = {
            "north_america": [
                "us",
                "usa",
                "united states",
                "canada",
                "mexico",
                "north american",
            ],
            "europe": [
                "europe",
                "european",
                "uk",
                "united kingdom",
                "france",
                "germany",
                "italy",
                "spain",
                "nordic",
            ],
            "latam": [
                "latam",
                "latin america",
                "brazil",
                "argentina",
                "colombia",
                "chile",
                "mexico",
            ],
            "asia": [
                "asia",
                "asian",
                "japan",
                "korea",
                "china",
                "india",
                "southeast asia",
            ],
            "oceania": ["oceania", "australia", "new zealand"],
            "africa": ["africa", "african"],
        }

    def _parse_date(self, date_string: str) -> Optional[datetime]:
        """Robustly parse date strings from various common formats."""
        if not date_string or not isinstance(date_string, str):
            return None
        
        try:
            # Use the more flexible dateutil_parse, which handles many formats
            return dateutil_parse(date_string)
        except (ValueError, TypeError, OverflowError):
            # Gracefully fail if parsing is not successful
            return None

    def extract_deal_info(self, article_text: str, article_date: str) -> Dict[str, Any]:
        """Extracts deal information from a single article, returning a list of deal objects."""
        doc = self.nlp(article_text)
        deals = []

        # Extract entities and features
        show_titles = [ent.text for ent in doc.ents if ent.label_ == "WORK_OF_ART"]
        broadcasters = [
            ent.text.strip()
            for ent in doc.ents
            if ent.label_ == "ORG"
            and not any(sub in ent.text.lower() for sub in self.non_broadcaster_orgs)
            and len(ent.text.strip()) > 2
            and "\n" not in ent.text
        ]
        deal_types = []
        for deal, keywords in self.deal_keywords.items():
            for keyword in keywords:
                if re.search(r"\b" + re.escape(keyword) + r"\b", article_text, re.IGNORECASE):
                    deal_types.append(deal)
                    break
        parsed_date = self._parse_date(article_date)
        deal_date = parsed_date.strftime("%Y-%m-%d") if parsed_date else None
        genres = []
        for genre, keywords in self.genre_keywords.items():
            for keyword in keywords:
                if re.search(r"\b" + re.escape(keyword) + r"\b", article_text, re.IGNORECASE):
                    if genre not in genres:
                        genres.append(genre)
                    break
        genres = sorted(list(set(genres)))
        regions = []
        for region, keywords in self.region_keywords.items():
            for keyword in keywords:
                if re.search(r"\b" + re.escape(keyword) + r"\b", article_text, re.IGNORECASE):
                    if region not in regions:
                        regions.append(region)
                    break
        regions = sorted(list(set(regions)))

        # Robust association: for each broadcaster and show, create a deal object
        # If no broadcasters or shows, still create a deal if other info is present
        if broadcasters and show_titles:
            for broadcaster in broadcasters:
                for show in show_titles:
                    deals.append({
                        "broadcaster": broadcaster,
                        "show": show,
                        "deal_type": deal_types[0] if deal_types else "other",
                        "deal_date": deal_date,
                        "genres": genres,
                        "regions": regions
                    })
        elif broadcasters:
            for broadcaster in broadcasters:
                deals.append({
                    "broadcaster": broadcaster,
                    "show": None,
                    "deal_type": deal_types[0] if deal_types else "other",
                    "deal_date": deal_date,
                    "genres": genres,
                    "regions": regions
                })
        elif show_titles:
            for show in show_titles:
                deals.append({
                    "broadcaster": None,
                    "show": show,
                    "deal_type": deal_types[0] if deal_types else "other",
                    "deal_date": deal_date,
                    "genres": genres,
                    "regions": regions
                })
        elif deal_types or genres or regions:
            deals.append({
                "broadcaster": None,
                "show": None,
                "deal_type": deal_types[0] if deal_types else "other",
                "deal_date": deal_date,
                "genres": genres,
                "regions": regions
            })
        return {"deals": deals}

    async def process_articles_from_mongodb(self):
        """Fetch articles from MongoDB, extract deal info, and store results back to DB."""
        await self.db_manager.connect()
        articles = await self.db_manager.get_all_articles(limit=1000)
        deals = []
        for article in articles:
            content = article.get("content", "")
            published_at = article.get("published_at", "")
            deal_info = self.extract_deal_info(content, published_at)
            for deal in deal_info["deals"]:
                deals.append({
                    "broadcaster_name": deal.get("broadcaster"),
                    "show_title": deal.get("show"),
                    "deal_type": deal.get("deal_type", "other"),
                    "publication_date": published_at,
                    "article_id": article.get("_id"),
                    "article_url": article.get("url"),
                    "genres": deal.get("genres", []),
                    "regions": deal.get("regions", []),
                    "source": article.get("source", ""),
                })
        if deals:
            await self.db_manager.upsert_deals_bulk(deals)
        await self.db_manager.close()
        print(f"Extracted and stored {len(deals)} deals from {len(articles)} articles.")
