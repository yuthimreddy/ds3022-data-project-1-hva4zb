# USING DBT

import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log')

logger = logging.getLogger(__name__)

# USING DBT
