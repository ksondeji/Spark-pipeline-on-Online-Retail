from dotenv import load_dotenv
import os

load_dotenv()

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
BRONZE_PATH = os.getenv("BRONZE_PATH")
SILVER_PATH = os.getenv("SILVER_PATH")
GOLD_PATH = os.getenv("GOLD_PATH")