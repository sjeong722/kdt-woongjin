import os
from dotenv import load_dotenv

load_dotenv()

SEOUL_API_KEY = os.getenv("SEOUL_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Default Target Lines to monitor (Can be expanded)
TARGET_LINES = [
    "1호선",
    "2호선",
    "3호선",
    "4호선",
    "5호선", 
    "6호선",
    "7호선",
    "8호선",
    "9호선",
    "경의중앙선",
    "공항철도",
    "분당선", 
    "신분당선"
]
