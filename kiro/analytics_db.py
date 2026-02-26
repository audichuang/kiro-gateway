import sqlite3
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from loguru import logger

# By default, use a data directory in the current working directory (project root)
DATA_DIR = Path("data")
ANALYTICS_DB_PATH = DATA_DIR / "analytics.sqlite3"

class AnalyticsDB:
    """
    SQLite-based storage for long-term API usage analytics.
    Tracks token consumption, duration, and status codes grouped by models and time.
    """
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or ANALYTICS_DB_PATH
        self._init_db()
        
    def _init_db(self):
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS token_usage (
                        id TEXT PRIMARY KEY,
                        timestamp REAL,
                        endpoint TEXT,
                        model TEXT,
                        prompt_tokens INTEGER,
                        completion_tokens INTEGER,
                        total_tokens INTEGER,
                        duration_ms REAL,
                        status_code INTEGER
                    )
                """)
                # Indexes for faster analytics queries
                conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON token_usage (timestamp DESC)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_model ON token_usage (model)")
        except Exception as e:
            logger.error(f"[AnalyticsDB] Failed to initialize database: {e}")

    def record_usage(self, 
                     record_id: str, 
                     timestamp: float, 
                     endpoint: str, 
                     model: str, 
                     prompt_tokens: int, 
                     completion_tokens: int,
                     duration_ms: float,
                     status_code: int):
        """Records a single API request's token usage."""
        try:
            total_tokens = prompt_tokens + completion_tokens
            # Normalize blanks
            model = model.strip() if model else "unknown"
            endpoint = endpoint.strip() if endpoint else "unknown"
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO token_usage 
                    (id, timestamp, endpoint, model, prompt_tokens, completion_tokens, total_tokens, duration_ms, status_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (record_id, timestamp, endpoint, model, prompt_tokens, completion_tokens, total_tokens, duration_ms, status_code))
        except Exception as e:
            logger.error(f"[AnalyticsDB] Error recording usage for {record_id}: {e}")

    def get_summary_stats(self) -> Dict[str, Any]:
        """Returns total requests, total tokens, overall stats and model breakdowns."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as total_requests,
                        SUM(prompt_tokens) as total_prompt_tokens,
                        SUM(completion_tokens) as total_completion_tokens,
                        SUM(total_tokens) as total_tokens
                    FROM token_usage
                """)
                row = cursor.fetchone()
                
                # Model distribution
                cursor = conn.execute("""
                    SELECT model, COUNT(*) as count, SUM(total_tokens) as tokens
                    FROM token_usage
                    GROUP BY model
                    ORDER BY tokens DESC
                """)
                models = [{"model": r[0], "requests": r[1], "tokens": r[2] or 0} for r in cursor.fetchall()]

                return {
                    "total_requests": row[0] or 0,
                    "total_prompt_tokens": row[1] or 0,
                    "total_completion_tokens": row[2] or 0,
                    "total_tokens": row[3] or 0,
                    "models": models
                }
        except Exception as e:
            logger.error(f"[AnalyticsDB] Error getting summary stats: {e}")
            return {"total_requests": 0, "total_prompt_tokens": 0, "total_completion_tokens": 0, "total_tokens": 0, "models": []}

    def get_trend_data(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Returns hourly trend data for the last `hours`."""
        try:
            cutoff = time.time() - (hours * 3600)
            with sqlite3.connect(self.db_path) as conn:
                # Group by hour. SQLite 'unixepoch' helps format the timestamp.
                cursor = conn.execute("""
                    SELECT 
                        strftime('%Y-%m-%d %H:00', datetime(timestamp, 'unixepoch', 'localtime')) as hour,
                        COUNT(*) as requests,
                        SUM(total_tokens) as tokens
                    FROM token_usage
                    WHERE timestamp >= ?
                    GROUP BY hour
                    ORDER BY hour ASC
                """, (cutoff,))
                
                return [{"hour": r[0], "requests": r[1], "tokens": r[2] or 0} for r in cursor.fetchall()]
        except Exception as e:
            logger.error(f"[AnalyticsDB] Error getting trend data: {e}")
            return []

# Global instance
analytics_db = AnalyticsDB()
