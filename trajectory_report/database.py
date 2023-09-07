from trajectory_report.config import DB, REDIS
from sqlalchemy import create_engine
import redis

DB_ENGINE = create_engine(DB)
REDIS_CONN = redis.Redis(REDIS)
