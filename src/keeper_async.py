"""This module deals with the database.
1. Ensure DB is ready
2. Fetch sites from DB
3. Insert stats into DB
"""
import os
from datetime import datetime, timezone
import re

from asyncio import Queue
import asyncio

import asyncpg
from utils import logger, KEEPER_SLEEP_INTERVAL, RUNNING_STATUS
from metrics import Stat
from endpoint import Endpoint

ENDPOINTS_TABLE_NAME = 'endpoints'
METRICS_TABLE_NAME = 'metrics'
PG_BATCH_SIZE = 1000

class Keeper:
    """
    This class deals with the database.
    """
    def __init__(self, metrics_buffer: Queue):
        self.metrics_buffer = metrics_buffer
        self.connection_pool = None

    async def run(self):
        """ Consume stats from metrics_buffer and insert them into DB."""
        # log the keeper's id
        logger.info(f"Keeper {id(self)} started")
        while RUNNING_STATUS:
            try:
                queue_size = self.metrics_buffer.qsize()
                if queue_size > 0:
                    metrics = []
                    consumer_length = queue_size if queue_size < PG_BATCH_SIZE else PG_BATCH_SIZE
                    logger.info(f"Keeper consumes {consumer_length}")
                    for _ in range(consumer_length):
                        # TODO There must be a better way
                        # to consume multiple items from a queue
                        metric = await self.metrics_buffer.get()
                        metrics.append(metric)
                        self.metrics_buffer.task_done()
                    self.insert_metrics(metrics)
            except Exception as e:
                logger.error(e)
            finally:
                await asyncio.sleep(KEEPER_SLEEP_INTERVAL)
        logger.info("Exiting a Keeper loop")

    async def get_connection_pool(self):
        if not self.connection_pool:
            self.connection_pool = await asyncpg.create_pool(
                min_size=1,
                max_size=10,
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
            )
        return self.connection_pool

    async def get_connection(self):
        """Get a connection from the pool."""
        pool = self.get_connection_pool()
        return await pool.acquire()

    async def release_connection(self, conn):
        """Manually release a connection back to the pool."""
        pool = self.get_connection_pool()
        await pool.release(conn)

    async def assure_endpoint_table(self):
        """Check if endpoint table is present"""
        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            # Open a transaction.
            async with conn.transaction():
                present = await conn.fetch(f"""SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = '{ENDPOINTS_TABLE_NAME}');
                    """)
                present = present[0]["exists"]
                if not present:
                    await conn.execute(
                        f"DROP SEQUENCE IF EXISTS {ENDPOINTS_TABLE_NAME}_endpoint_id_seq CASCADE;"
                        )
                    await conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {ENDPOINTS_TABLE_NAME} (
                            endpoint_id SERIAL PRIMARY KEY,
                            url VARCHAR(255) not null,
                            regex VARCHAR(255),
                            interval INT CHECK (interval >= 5 AND interval <= 300)
                            );
                        """)
    
    async def assure_metrics_table(self):
        """Check if metrics table is present and a hypertable. If not, create one."""

        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            # Open a transaction.
            async with conn.transaction():
                await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {METRICS_TABLE_NAME} (
                        start_time TIMESTAMPTZ NOT NULL,
                        endpoint_id integer REFERENCES {ENDPOINTS_TABLE_NAME}(endpoint_id),
                        duration REAL NOT NULL,
                        status_code INT,
                        regex_match BOOLEAN,
                        PRIMARY KEY (start_time, endpoint_id)
                    );
                """)

                # Check if metrics table is a hypertable
                is_hypertable = await conn.fetch(f"""
                    SELECT EXISTS (
                        SELECT 1 FROM timescaledb_information.hypertables
                        WHERE hypertable_name = '{METRICS_TABLE_NAME}'
                    );
                    """)
                is_hypertable = is_hypertable[0]["exists"]
                if not is_hypertable:
                    await conn.execute(f"SELECT create_hypertable('{METRICS_TABLE_NAME}', 'start_time');")

    async def fetch_endpoints(self, partition_count: int, partition_id: int):
        """Fetch URLs to be monitored from a DB table.
        Multiple instances of this program can be run to utilize multiple cores.
        Each instance will fetch a different set of URLs.
        """
        endpoints = []
        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            # Open a transaction.
            async with conn.transaction():
                select_sql = f"""SELECT endpoint_id, url, regex, interval FROM 
                    {ENDPOINTS_TABLE_NAME} 
                    where endpoint_id % {partition_count} = {partition_id};"""
                async for row in conn.cursor(select_sql):
                    try:
                        endpoints.append(Endpoint(row[0], row[1], row[2], row[3]))
                    except re.error as e:
                        # Invalid regex will be ignored
                        logger.warning(e)
                        continue
        return endpoints

    async def check_readiness(self):
        """Ensure DB tables are ready."""
        await self.assure_endpoint_table()
        await self.assure_metrics_table()
        return self

    async def insert_metrics(self, metrics: list[Stat]):
        """Insert metrics into DB."""
        if len(metrics) == 0:
            return
        
        pool = await self.get_connection_pool()
        async with pool.acquire() as conn:
            # Open a transaction.
            async with conn.transaction():
                insert = f"""
                    INSERT INTO {METRICS_TABLE_NAME}
                    (start_time, endpoint_id, duration, status_code, regex_match)
                    VALUES ($1, $2, $3, $4, $5);
                    """
                values = [(  datetime.fromtimestamp(metric.start_time, timezone.utc),
                    metric.endpoint.endpoint_id,
                    metric.duration,
                    metric.status_code,
                    metric.regex_match) for metric in metrics]
                await conn.executemany(insert, values)

    def __del__(self):
        self.connection_pool = None
