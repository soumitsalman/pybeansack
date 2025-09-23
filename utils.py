import os
from datetime import datetime, timezone, timedelta

VECTOR_LEN = int(os.getenv('VECTOR_LEN', 384))
CLUSTER_EPS = float(os.getenv('CLUSTER_EPS', 0.3))

now = lambda: datetime.now(timezone.utc)
ndays_ago = lambda ndays: now() - timedelta(days=ndays)
ndays_ago_str = lambda ndays: ndays_ago(ndays).strftime('%Y-%m-%d')