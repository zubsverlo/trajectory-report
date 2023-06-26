from trajectory_report.config import DB
from sqlalchemy import create_engine

DB_ENGINE = create_engine(DB)
