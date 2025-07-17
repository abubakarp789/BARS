# Configuration constants for the grading engine

grade_thresholds = {
    "A": 60,  # Active in the last 2 months
    "B": 180,  # Active in the last 6 months
    "C": 365,  # Active in the last year
    "D": float("inf"),  # Older than 1 year
}

deal_type_weights = {
    "acquisition": 1.0,
    "commission": 1.2,
    "co-production": 1.1,
    "licensing": 0.9,
    "renewal": 0.8,
    "development": 0.7,
    "other": 0.5,
} 