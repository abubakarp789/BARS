"""
Enhanced Grading Engine

This module aggregates deal information from MongoDB, calculates a grade and score
for each broadcaster based on recent activity, and saves the results.
"""

import json
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from dataclasses import dataclass, field
import logging
from .mongodb_manager import MongoDBManager
from dotenv import load_dotenv
import os
from src.bars.core.config import grade_thresholds, deal_type_weights

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
from dateutil.parser import isoparse

# Define the absolute path for the output file
output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data'))
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "broadcaster_grades.json")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
# load_dotenv() # This line is now redundant as load_dotenv is called above


@dataclass
class BroadcasterGrade:
    """A dataclass to hold the calculated grade and metrics for a broadcaster."""

    broadcaster_name: str
    grade: str
    score: float
    last_activity_date: str
    deal_count: int
    recent_deals: List[Dict]
    deal_types: List[str]
    shows: List[str]
    genres: List[str] = field(default_factory=list)
    regions: List[str] = field(default_factory=list)
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class EnhancedGradingEngine:
    """Calculates broadcaster grades based on deal data from the database."""

    def __init__(self):
        """Initialize the grading engine with MongoDB connection and scoring rules."""
        self.db_manager = MongoDBManager()

        self.max_recent_deals = 5
        self.max_shows = 10

    def _calculate_grade(self, days_since_last_activity: int) -> str:
        """Determines a letter grade based on the days since the last deal."""
        if days_since_last_activity <= grade_thresholds["A"]:
            return "A"
        elif days_since_last_activity <= grade_thresholds["B"]:
            return "B"
        elif days_since_last_activity <= grade_thresholds["C"]:
            return "C"
        else:
            return "D"

    def _calculate_score(
        self, grade: str, num_deals: int, deal_types: List[str]
    ) -> float:
        """Calculates a numerical score for fine-grained ranking."""
        base_scores = {"A": 100, "B": 80, "C": 60, "D": 20}

        # Average the weights of all deal types found
        type_multiplier = sum(
            deal_type_weights.get(dt, 0.5) for dt in deal_types
        ) / max(1, len(deal_types))

        # Add a bonus for the volume of deals, capped at 20 points
        deal_bonus = min(num_deals * 2, 20)

        score = (base_scores.get(grade, 0) * type_multiplier) + deal_bonus
        return round(score, 2)

    async def run_grading_pipeline(self) -> Dict[str, Dict]:
        """
        The main function to run the entire grading process using an efficient
        MongoDB aggregation pipeline.
        """
        logger.info("Starting BARS grading pipeline with MongoDB aggregation...")
        await self.db_manager.connect()
        now = datetime.now(timezone.utc)

        # The aggregation pipeline is now handled by the MongoDBManager
        broadcaster_cursor = self.db_manager.aggregate_deals_by_broadcaster()

        final_grades = {}
        async for broadcaster_data in broadcaster_cursor:
            broadcaster = broadcaster_data.get("_id")
            if not broadcaster:
                continue

            try:
                # Most data is pre-calculated by the aggregation pipeline
                deals_sorted = broadcaster_data.get("deals", [])
                latest_deal_date = broadcaster_data.get("last_activity_date")

                if not latest_deal_date:
                    logger.warning(
                        f"Skipping {broadcaster} due to missing activity date."
                    )
                    continue

                # --- START OF THE FIX ---
                # Convert the date string into a real datetime object
                try:
                    if isinstance(latest_deal_date, str):
                        latest_deal_date_obj = datetime.fromisoformat(latest_deal_date)
                    elif isinstance(latest_deal_date, datetime):
                        # If it's already a datetime object, ensure it's timezone-aware
                        latest_deal_date_obj = (
                            latest_deal_date.replace(tzinfo=timezone.utc)
                            if latest_deal_date.tzinfo is None
                            else latest_deal_date
                        )
                    else:
                        raise TypeError(
                            f"Unsupported date type: {type(latest_deal_date)}"
                        )

                except (ValueError, TypeError) as e:
                    logger.error(
                        f"Could not parse date for {broadcaster}. Invalid format: {latest_deal_date}. Error: {e}"
                    )
                    continue  # Skip this broadcaster
                # --- END OF THE FIX ---

                days_since_last = (now - latest_deal_date_obj).days
                grade = self._calculate_grade(days_since_last)

                all_deal_types = sorted(
                    list({d.get("deal_type", "other") for d in deals_sorted})
                )
                
                # Corrected line (filters out None before sorting):
                all_shows_set = {d.get("show_title") for d in deals_sorted}
                all_shows = sorted([show for show in all_shows_set if show is not None])

                score = self._calculate_score(
                    grade, broadcaster_data.get("deal_count", 0), all_deal_types
                )
                
                # --- FIX FOR ISOFORMAT ATTRIBUTE ERROR ---
                deals_info = []
                for d in deals_sorted[: self.max_recent_deals]:
                    pub_date_str = d.get("publication_date")
                    pub_date_iso = None
                    if pub_date_str:
                        try:
                            # Convert the date string before calling isoformat()
                            if isinstance(pub_date_str, str):
                                pub_date_iso = datetime.fromisoformat(
                                    pub_date_str
                                ).isoformat()
                            elif isinstance(pub_date_str, datetime):
                                pub_date_iso = pub_date_str.isoformat()
                        except (ValueError, TypeError):
                            # If conversion fails, leave it as None
                            pub_date_iso = None

                    deals_info.append(
                        {
                            "show_title": d.get("show_title"),
                            "deal_type": d.get("deal_type"),
                            "date": pub_date_iso,  # Use the converted date
                            "source": d.get("source"),
                            "article_url": d.get("article_url"),
                        }
                    )


                grade_obj = BroadcasterGrade(
                    broadcaster_name=broadcaster,
                    grade=grade,
                    score=score,
                    last_activity_date=latest_deal_date_obj.isoformat(),
                    deal_count=broadcaster_data.get("deal_count", 0),
                    recent_deals=deals_info,
                    deal_types=all_deal_types,
                    shows=all_shows[: self.max_shows],
                    genres=sorted(
                        list(set(g for d in deals_sorted for g in d.get("genres", [])))
                    ),
                    regions=sorted(
                        list(set(r for d in deals_sorted for r in d.get("regions", [])))
                    ),
                )

                grade_dict = {
                    **grade_obj.__dict__,
                    "updated_at": grade_obj.updated_at.isoformat(),
                }
                final_grades[broadcaster] = grade_dict

                await self.db_manager.upsert_grade(grade_dict)

            except Exception as e:
                logger.error(
                    f"Failed to calculate grade for {broadcaster}: {e}", exc_info=True
                )

        logger.info(
            f"Grading pipeline completed. Processed {len(final_grades)} broadcasters."
        )

        self._print_summary(final_grades)

        output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'broadcaster_grades.json')
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(final_grades, f, ensure_ascii=False, indent=2)
        logger.info(f"Results saved to {output_file}")

        await self.db_manager.close()
        return final_grades

    def _print_summary(self, broadcaster_grades: Dict[str, Dict]):
        """Prints a formatted summary of the grading results to the console."""
        if not broadcaster_grades:
            return

        grade_dist = {"A": 0, "B": 0, "C": 0, "D": 0}
        for data in broadcaster_grades.values():
            grade = data.get("grade", "D")
            if grade in grade_dist:
                grade_dist[grade] += 1

        sorted_broadcasters = sorted(
            broadcaster_grades.values(),
            key=lambda x: (x.get("grade", "Z"), -x.get("score", 0)),
        )

        print("\n" + "=" * 60)
        print("BROADCASTER ACTIVITY RATING SYSTEM (BARS) - SUMMARY")
        print("=" * 60)
        print("\nGRADE DISTRIBUTION:")
        for grade, count in grade_dist.items():
            print(f"  Grade {grade}: {count} broadcasters")

        print("\nTOP BROADCASTERS BY GRADE:")
        for bc in sorted_broadcasters[:15]:  # Print top 15
            print(
                f"  - [{bc['grade']}] {bc['broadcaster_name']} (Score: {bc['score']:.2f}, Deals: {bc['deal_count']})"
            )
        print("\n" + "=" * 60)


if __name__ == "__main__":

    async def main_async():
        grading_engine = EnhancedGradingEngine()
        try:
            await grading_engine.run_grading_pipeline()
        except Exception as e:
            logger.error(
                f"An error occurred in the main execution block: {e}", exc_info=True
            )

    asyncio.run(main_async())
