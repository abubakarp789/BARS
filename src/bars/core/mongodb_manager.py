"""
Asynchronous MongoDB Manager

This module provides a robust, high-performance, and secure manager for all
MongoDB interactions in the BARS project. It uses efficient bulk operations
and correctly configured indexes.
"""

import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorDatabase,
    AsyncIOMotorCollection,
)
from pymongo import IndexModel, ASCENDING, DESCENDING, UpdateOne
from pymongo.errors import (
    ConnectionFailure,
    OperationFailure,
    ServerSelectionTimeoutError,
    BulkWriteError,
)
from bson import ObjectId

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MongoDBManager:
    """Manages all asynchronous connections and operations with the MongoDB database."""

    def __init__(self):
        """
        Initializes the MongoDB manager.
        It requires MONGODB_URI and DATABASE_NAME to be set as environment variables.
        """
        self.connection_uri = os.getenv("MongoDB_URI")
        self.db_name = os.getenv("DATABASE_NAME", "bars")

        if not self.connection_uri:
            raise ValueError(
                "MONGODB_URI environment variable not set. Cannot connect to the database."
            )

        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None

        # Collections will be initialized upon connection
        self.articles: Optional[AsyncIOMotorCollection] = None
        self.deals: Optional[AsyncIOMotorCollection] = None
        self.grades: Optional[AsyncIOMotorCollection] = None

    async def connect(self):
        """Establishes a connection to MongoDB and initializes collections and indexes."""
        if self.client:
            return

        try:
            logger.info("Attempting to connect to MongoDB...")
            self.client = AsyncIOMotorClient(
                self.connection_uri,
                serverSelectionTimeoutMS=10000,  # Increased timeout for better resilience
            )
            await self.client.admin.command("ping")
            logger.info("Successfully connected to MongoDB.")

            self.db = self.client[self.db_name]
            await self._initialize_collections()

        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(
                f"Failed to connect to MongoDB. Check your connection string and firewall settings. Error: {e}"
            )
            raise

    async def _initialize_collections(self):
        """Initializes collection objects and ensures indexes are created."""
        if self.db is None:
            raise RuntimeError("Database not initialized. Call connect() first.")

        self.articles = self.db["articles"]
        self.deals = self.db["deals"]
        self.grades = self.db["grades"]

        logger.info(f"Initializing collections in database: {self.db_name}")
        await self._create_indexes()

    async def _create_indexes(self):
        """Creates indexes on collections to ensure efficient queries."""
        if self.articles is None or self.deals is None or self.grades is None:
            raise RuntimeError("Collections not initialized.")

        logger.info("Creating database indexes...")
        await self.articles.create_indexes(
            [
                IndexModel([("url", ASCENDING)], unique=True),
                IndexModel([("published_at", DESCENDING)]),
                IndexModel([("source", ASCENDING)]),
            ]
        )

        await self.deals.create_indexes(
            [
                IndexModel([("broadcaster_name", ASCENDING)]),
                IndexModel([("publication_date", DESCENDING)]),
                IndexModel([("article_id", ASCENDING)]),
            ]
        )

        await self.grades.create_indexes(
            [
                IndexModel([("broadcaster_name", ASCENDING)], unique=True),
                IndexModel([("updated_at", DESCENDING)]),
                IndexModel([("score", DESCENDING)]),
            ]
        )
        logger.info("Database indexes created successfully.")

    # --- EFFICIENT BULK WRITE OPERATIONS ---

    async def upsert_articles_bulk(self, articles_data: List[Dict[str, Any]]):
        """Efficiently inserts or updates a list of articles using a single bulk operation."""
        if not articles_data:
            return
        now = datetime.now(timezone.utc)
        operations = [
            UpdateOne(
                {"url": article["url"]},
                {
                    "$set": {**article, "updated_at": now},
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
            for article in articles_data
            if "url" in article
        ]
        if not operations:
            return
        try:
            result = await self.articles.bulk_write(operations, ordered=False)
            logger.info(
                f"Articles bulk write: {result.upserted_count} new, {result.modified_count} updated."
            )
        except BulkWriteError as bwe:
            logger.error(f"Error during articles bulk write: {bwe.details}")

    async def upsert_deals_bulk(self, deals_data: List[Dict[str, Any]]):
        """Efficiently inserts or updates a list of deals using a single bulk operation."""
        if not deals_data:
            return
        now = datetime.now(timezone.utc)
        operations = [
            UpdateOne(
                {
                    "article_id": deal["article_id"],
                    "broadcaster_name": deal["broadcaster_name"],
                    "show_title": deal["show_title"],
                    "deal_type": deal["deal_type"],
                },
                {
                    "$set": {**deal, "updated_at": now},
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
            for deal in deals_data
        ]
        try:
            result = await self.deals.bulk_write(operations, ordered=False)
            logger.info(
                f"Deals bulk write: {result.upserted_count} new, {result.modified_count} updated."
            )
        except BulkWriteError as bwe:
            logger.error(f"Error during deals bulk write: {bwe.details}")

    async def upsert_grade(self, grade_data: Dict[str, Any]):
        """Inserts or updates a single broadcaster's grade."""
        if "broadcaster_name" not in grade_data:
            return
        try:
            await self.grades.update_one(
                {"broadcaster_name": grade_data["broadcaster_name"]},
                {"$set": grade_data},
                upsert=True,
            )
        except Exception as e:
            logger.error(
                f"Error upserting grade for {grade_data.get('broadcaster_name')}: {e}"
            )

    # --- DATA READ OPERATIONS ---

    async def get_all_articles(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Gets recent articles from the database, sorted by publication date."""
        if self.articles is None:
            return []
        cursor = self.articles.find().sort("published_at", DESCENDING).limit(limit)
        return [self._convert_objectid_to_str(doc) async for doc in cursor]

    async def get_all_deals(self) -> List[Dict[str, Any]]:
        """Gets all deals from the database."""
        if self.deals is None:
            return []
        cursor = self.deals.find().sort("publication_date", DESCENDING)
        return [self._convert_objectid_to_str(doc) async for doc in cursor]

    async def get_all_grades(self) -> List[Dict[str, Any]]:
        """Gets all broadcaster grades from the database."""
        if self.grades is None:
            return []
        cursor = self.grades.find().sort("score", DESCENDING)
        return [self._convert_objectid_to_str(doc) async for doc in cursor]

    async def get_database_stats(self) -> Dict[str, int]:
        """Gets counts of documents in each collection."""
        if self.db is None:
            return {}
        stats = {}
        try:
            stats["articles_count"] = await self.db.articles.count_documents({})
            stats["deals_count"] = await self.db.deals.count_documents({})
            stats["grades_count"] = await self.db.grades.count_documents({})
            stats["broadcasters_count"] = await self.db.grades.distinct(
                "broadcaster_name"
            )
            stats["broadcasters_count"] = len(stats["broadcasters_count"])
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
        return stats

    def aggregate_deals_by_broadcaster(self):
        """
        Uses the MongoDB aggregation framework to efficiently group deals by
        broadcaster, pre-calculating key metrics on the database side.
        """
        pipeline = [
            {
                # Stage 1: Filter out documents that are missing essential fields
                "$match": {
                    "broadcaster_name": {"$ne": None, "$exists": True},
                    "publication_date": {"$ne": None, "$exists": True},
                }
            },
            {
                # Stage 2: Sort by date so we can easily find the most recent deal
                "$sort": {"publication_date": -1}
            },
            {
                # Stage 3: Group by broadcaster to perform calculations
                "$group": {
                    "_id": "$broadcaster_name",
                    "last_activity_date": {"$first": "$publication_date"},
                    "deal_count": {"$sum": 1},
                    # Push the full deal documents into an array for later processing
                    "deals": {"$push": "$$ROOT"}
                }
            }
        ]
        return self.deals.aggregate(pipeline)

    # --- UTILITY AND CONTEXT MANAGEMENT ---

    @staticmethod
    def _convert_objectid_to_str(
        doc: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """Converts a document's '_id' field from ObjectId to string."""
        if doc and "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc

    async def close(self):
        """Closes the MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed.")

    async def __aenter__(self):
        """Async context manager entry: connects to DB."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit: closes connection."""
        await self.close()
