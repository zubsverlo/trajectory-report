from trajectory_report.settings import DB
from sqlalchemy import create_engine

DB_ENGINE = create_engine(DB)
