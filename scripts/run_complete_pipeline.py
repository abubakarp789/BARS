#!/usr/bin/env python3
"""
Enhanced Complete Pipeline Runner for BARS (Broadcaster Activity Rating System)

This script orchestrates the complete data pipeline:
1. Scrapes articles from multiple sources.
2. Stores articles efficiently in MongoDB.
3. Extracts deal information using enhanced NLP.
4. Calculates broadcaster grades using an advanced scoring algorithm.
5. Generates a summary report of the execution.

Usage:
    python run_complete_pipeline.py [--sources SOURCE1,SOURCE2] [--test-mode]
"""

import os
import sys
import argparse
import asyncio
import time
from datetime import datetime
from dotenv import load_dotenv

# --- SETUP AND IMPORTS ---

# Load environment variables from .env file at the very beginning
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

# Add project directories to the Python path to ensure correct imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.insert(0, project_root)

try:
    from src.bars.core.mongodb_manager import MongoDBManager
    from src.bars.core.nlp_extractor import EnhancedNLPExtractor
    from src.bars.core.grading_engine import EnhancedGradingEngine

    # Import the scraper modules directly for clarity
    from src.bars.scrapers import animation_magazine_scraper
    from src.bars.scrapers import kidscreen_scraper
    from src.bars.scrapers import c21media_scraper
    from src.bars.scrapers import variety_scraper
except ImportError as e:
    print(f"Import error: {e}")
    print("Please ensure all required modules and their dependencies are installed.")
    sys.exit(1)


class EnhancedPipelineRunner:
    """
    Orchestrates the complete BARS data processing workflow.
    """

    def __init__(self, test_mode=False):
        """Initialize the pipeline runner."""
        self.test_mode = test_mode
        self.db_manager = None
        self.nlp_extractor = None
        self.grading_engine = None
        self.start_time = datetime.now()

        print("Initializing Enhanced BARS Pipeline Runner")
        print(f"Started at: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Test mode: {'Enabled' if test_mode else 'Disabled'}")
        print("-" * 60)

    async def initialize_components(self) -> bool:
        """
        Initialize and connect all pipeline components.
        """
        try:
            print("Initializing pipeline components...")

            self.db_manager = MongoDBManager()
            await self.db_manager.connect()
            print("MongoDB manager initialized and connected")

            self.nlp_extractor = EnhancedNLPExtractor()
            print("Enhanced NLP extractor initialized")

            self.grading_engine = EnhancedGradingEngine()
            print("Enhanced grading engine initialized")

            return True

        except Exception as e:
            print(f"Failed to initialize components: {e}")
            return False

    async def run_scraping_phase(self, sources=None) -> int:
        """
        Run the scraping phase for all or specified sources concurrently.
        """
        print("\n" + "=" * 60)
        print("PHASE 1: ARTICLE SCRAPING")
        print("=" * 60)

        all_sources = {
            "animation_magazine": animation_magazine_scraper.scrape_animation_magazine,
            "kidscreen": kidscreen_scraper.scrape_kidscreen,
            "c21media": c21media_scraper.scrape_c21media,
            "variety": variety_scraper.scrape_variety,
        }

        # Determine which sources to run
        sources_to_run = sources or all_sources.keys()

        tasks = [
            all_sources[src](test_mode=self.test_mode, nlp_extractor=self.nlp_extractor)
            for src in sources_to_run
            if src in all_sources
        ]

        if not tasks:
            print("No valid sources selected to scrape.")
            return 0

        all_articles = []
        print(f"Running scrapers for: {', '.join(sources_to_run)}")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, list):
                all_articles.extend(result)
            elif isinstance(result, Exception):
                print(f"A scraper failed with an error: {result}")

        if all_articles:
            print(f"\nTotal articles collected from all sources: {len(all_articles)}")
            await self.db_manager.upsert_articles_bulk(all_articles)
        else:
            print("\nNo articles were collected from any source.")

        return len(all_articles)

    async def run_nlp_extraction_phase(self):
        """
        Run the NLP extraction phase to find deals in articles.
        """
        print("\n" + "=" * 60)
        print("PHASE 2: NLP EXTRACTION")
        print("=" * 60)

        try:
            await self.nlp_extractor.process_articles_from_mongodb()
            print("NLP extraction completed successfully")
            return True
        except Exception as e:
            print(f"NLP extraction failed: {e}")
            return False

    async def run_grading_phase(self):
        """
        Run the grading phase to score broadcasters.
        """
        print("\n" + "=" * 60)
        print("PHASE 3: BROADCASTER GRADING")
        print("=" * 60)

        try:
            grades = await self.grading_engine.run_grading_pipeline()
            if grades:
                print("Grading phase completed successfully")
                return True, grades
            else:
                print(
                    "Grading phase completed, but no grades were generated (this may be expected if no new deals were found)."
                )
                return True, None
        except Exception as e:
            print(f"Grading phase failed: {e}")
            return False, None

    async def generate_summary_report(self, grades):
        """
        Generate a final summary report of the pipeline execution.
        """
        print("\n" + "=" * 60)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 60)

        end_time = datetime.now()
        duration = end_time - self.start_time

        stats = await self.db_manager.get_database_stats()

        print(f"Execution Time: {str(duration).split('.')[0]}")
        print(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(
            f"Database Stats: {stats.get('articles_count', 0)} Articles | {stats.get('deals_count', 0)} Deals | {stats.get('grades_count', 0)} Grades"
        )

        if grades:
            grade_dist = {}
            for data in grades.values():
                grade = data["grade"]
                grade_dist[grade] = grade_dist.get(grade, 0) + 1

            print("\nGrade Distribution:")
            for grade in ["A", "B", "C", "D"]:
                count = grade_dist.get(grade, 0)
                print(f"   Grade {grade}: {count} broadcasters")

        print("\n" + "=" * 60)

    async def run_complete_pipeline(self, sources=None):
        """
        Run the complete BARS data pipeline from start to finish.
        """
        try:
            if not await self.initialize_components():
                return False

            total_articles = await self.run_scraping_phase(sources)

            if total_articles == 0 and not self.test_mode:
                print(
                    "\nNo new articles were scraped. Skipping NLP and Grading phases."
                )
                await self.generate_summary_report(None)
                await self.cleanup()
                return True

            nlp_success = await self.run_nlp_extraction_phase()
            grading_success, grades = await self.run_grading_phase()

            await self.generate_summary_report(grades)

            success = nlp_success and grading_success

            if success:
                print("\nPipeline completed successfully!")
                print("You can now run the dashboard: streamlit run dashboard.py")
            else:
                print("\nPipeline completed with some issues.")

            return success

        except Exception as e:
            print(f"A critical error occurred in the pipeline: {e}")
            return False

        finally:
            await self.cleanup()

    async def cleanup(self):
        """
        Clean up resources, primarily the database connection.
        """
        print("\nCleaning up resources...")
        if self.db_manager:
            await self.db_manager.close()
        print("Cleanup completed")


async def main_async():
    """
    Parses command-line arguments and runs the async pipeline.
    """
    parser = argparse.ArgumentParser(
        description="Enhanced BARS Complete Pipeline Runner"
    )
    parser.add_argument(
        "--sources",
        type=str,
        help="Comma-separated list of sources to scrape (e.g., kidscreen,variety)",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Run in test mode without actual scraping",
    )

    args = parser.parse_args()

    sources = args.sources.split(",") if args.sources else None
    if sources:
        print(f"Selected sources: {sources}")

    pipeline = EnhancedPipelineRunner(test_mode=args.test_mode)

    try:
        return await pipeline.run_complete_pipeline(sources)
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user.")
        return False


def main():
    """
    Main entry point that handles the asyncio event loop.
    """
    try:
        success = asyncio.run(main_async())
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\nA fatal, unhandled error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
