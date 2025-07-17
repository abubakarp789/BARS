"""
Enhanced Broadcaster Activity Rating System (BARS)

A comprehensive AI-powered tool for analyzing broadcaster activity in the 
animated and kids' content industry.
"""

__version__ = "2.0.0"
__author__ = "Abu Bakar"
__email__ = "abubakarp789@gmail.com"
__description__ = "Enhanced Broadcaster Activity Rating System"

# Import main components for easy access
try:
    from core.mongodb_manager import MongoDBManager
    from core.enhanced_nlp_extractor import EnhancedNLPExtractor
    from core.enhanced_grading_engine import EnhancedGradingEngine
except ImportError:
    # Handle import errors gracefully
    pass

# Package metadata
__all__ = ["MongoDBManager", "EnhancedNLPExtractor", "EnhancedGradingEngine"]
