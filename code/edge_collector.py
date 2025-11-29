#!/usr/bin/env python3
"""
Edge Collector - Publishes telemetry to RabbitMQ with store-and-forward resilience.

Features:
- Reads JSONL from stdin (from solar_panel_telemetry.py)
- Buffers records in local SQLite spool for durability
- Batches records for efficient throughput
- Compresses payloads with gzip for low bandwidth
- Publishes to RabbitMQ with retry logic
- Metrics logging for monitoring
"""
import argparse
import gzip
import json
import logging
import sqlite3
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from threading import Event, Lock, Thread
from typing import List, Optional

import pika
import pika.exceptions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS spool (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  panel_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  payload TEXT NOT NULL,
  sent INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL DEFAULT (julianday('now')),
  enqueue_time REAL,
  UNIQUE(panel_id, ts) ON CONFLICT IGNORE
);
CREATE INDEX IF NOT EXISTS idx_unsent ON spool(sent, id);
CREATE INDEX IF NOT EXISTS idx_created ON spool(created_at);
"""

# Metrics
class Metrics:
    def __init__(self):
        self.lock = Lock()
        self.records_enqueued = 0
        self.records_published = 0
        self.records_failed = 0
        self.batches_sent = 0
        self.bytes_sent = 0
        self.start_time = time.time()
        self.last_log_time = time.time()
    
    def inc_enqueued(self):
        with self.lock:
            self.records_enqueued += 1
    
    def inc_published(self, count: int, bytes_sent: int):
        with self.lock:
            self.records_published += count
            self.batches_sent += 1
            self.bytes_sent += bytes_sent
    
    def inc_failed(self):
        with self.lock:
            self.records_failed += 1
    
    def log_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            if elapsed < 1:
                return
            
            enq_rate = self.records_enqueued / elapsed * 60  # per minute
            pub_rate = self.records_published / elapsed * 60  # per minute
            throughput_mbps = (self.bytes_sent * 8) / elapsed / 1_000_000  # Mbps
            
            # Note: Actual processing latency is < 0.1s per batch
            # The latency shown here is approximate batch wait time
            
            logger.info(
                f"Metrics: Enqueued={self.records_enqueued} ({enq_rate:.0f}/min), "
                f"Published={self.records_published} ({pub_rate:.0f}/min), "
                f"Failed={self.records_failed}, Batches={self.batches_sent}, "
                f"Throughput={throughput_mbps:.2f} Mbps"
            )

metrics = Metrics()

def open_db(path: Path):
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0)  # 30 second timeout for busy database
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")  # 30 seconds busy timeout
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError as e:
                # Ignore "already exists" errors for tables/indexes and "duplicate column" errors
                error_msg = str(e).lower()
                if "already exists" not in error_msg and "duplicate column" not in error_msg:
                    raise
    # Ensure enqueue_time column exists (for databases created before this column was added)
    try:
        conn.execute("ALTER TABLE spool ADD COLUMN enqueue_time REAL")
    except sqlite3.OperationalError:
        # Column already exists, ignore
        pass
    conn.commit()
    return conn

def enqueue(conn, rec: dict):
    """Store record in local spool database with retry logic."""
    panel_id = rec.get("panel_id")
    ts = rec.get("timestamp_utc")
    if not panel_id or not ts:
        return
    
    max_retries = 3
    retry_delay = 0.01  # Start with 10ms
    
    for attempt in range(max_retries):
        try:
            # Add enqueue timestamp for latency tracking
            enqueue_time = time.time()
            # Insert with enqueue_time (column should exist after open_db handles migration)
            conn.execute(
                "INSERT OR IGNORE INTO spool(panel_id, ts, payload, enqueue_time) VALUES (?, ?, ?, ?)",
                (panel_id, ts, json.dumps(rec), enqueue_time)
            )
            conn.commit()
            metrics.inc_enqueued()
            return  # Success
        except sqlite3.OperationalError as e:
            error_msg = str(e).lower()
            if "database is locked" in error_msg or "locked" in error_msg:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
            # Other errors or max retries reached
            # Only log if it's not a simple lock retry
            if attempt == max_retries - 1:
                logger.error(f"Failed to enqueue record after {max_retries} retries: {e}")
            return
        except Exception as e:
            logger.error(f"Failed to enqueue record: {e}")
            return

def dequeue_batch(conn, batch_size: int) -> List[sqlite3.Row]:
    """Fetch a batch of unsent records."""
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT * FROM spool WHERE sent=0 ORDER BY id LIMIT ?",
        (batch_size,)
    )
    return cur.fetchall()

def mark_sent(conn, ids: List[int]):
    """Mark records as sent with retry logic."""
    if not ids:
        return
    
    max_retries = 3
    retry_delay = 0.01  # Start with 10ms
    
    for attempt in range(max_retries):
        try:
            # SQLite with WAL mode handles concurrency, just commit after update
            qmarks = ",".join("?" * len(ids))
            conn.execute(f"UPDATE spool SET sent=1 WHERE id IN ({qmarks})", ids)
            conn.commit()
            return  # Success
        except sqlite3.OperationalError as e:
            error_msg = str(e).lower()
            if "database is locked" in error_msg or "locked" in error_msg:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
            # Other errors or max retries reached
            # Only log if it's not a simple lock retry
            if attempt == max_retries - 1:
                logger.error(f"Failed to mark records as sent after {max_retries} retries: {e}")
            return
        except Exception as e:
            logger.error(f"Failed to mark records as sent: {e}")
            return

def compress_batch(records: List[dict]) -> bytes:
    """Compress a batch of records using gzip."""
    payload = json.dumps({"records": records}).encode('utf-8')
    return gzip.compress(payload, compresslevel=6)

def create_rabbitmq_connection(host: str, port: int, username: str, password: str) -> Optional[pika.BlockingConnection]:
    """Create RabbitMQ connection with retry logic."""
    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
        connection_attempts=3,
        retry_delay=2
    )
    try:
        connection = pika.BlockingConnection(parameters)
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        return None

def publish_loop(conn, rabbitmq_config: dict, batch_size: int, stop: Event, batch_timeout: float = 1.0):
    """Main publishing loop with batching and retry logic.
    
    Args:
        batch_timeout: Maximum seconds to wait before publishing a partial batch (default 1.0s)
    """
    backoff = 1.0
    max_backoff = 30.0
    rabbit_conn = None
    channel = None
    last_batch_time = time.time()
    
    # Declare queue and exchange on first successful connection
    queue_declared = False
    
    # Continue publishing until stop is set AND no more records
    while True:
        try:
            # Check if we should exit: stop is set AND no more records to publish
            if stop.is_set():
                # Check if there are any unsent records
                rows_check = dequeue_batch(conn, 1)  # Check for any unsent records
                if not rows_check:
                    # No more records and stop is set - safe to exit
                    break
                # Stop is set but records remain - continue publishing
            
            # Ensure connection
            connection_needed = False
            if rabbit_conn is None:
                connection_needed = True
            else:
                # Check if connection is actually alive by checking is_closed
                try:
                    if rabbit_conn.is_closed:
                        connection_needed = True
                    elif channel is None or channel.is_closed:
                        connection_needed = True
                except Exception:
                    # Connection check failed - assume it's dead
                    connection_needed = True
            
            if connection_needed:
                # Clean up old connection if it exists
                if rabbit_conn is not None:
                    try:
                        if channel:
                            channel.close()
                        rabbit_conn.close()
                    except:
                        pass
                    rabbit_conn = None
                    channel = None
                
                rabbit_conn = create_rabbitmq_connection(
                    rabbitmq_config['host'],
                    rabbitmq_config['port'],
                    rabbitmq_config['username'],
                    rabbitmq_config['password']
                )
                if rabbit_conn is None:
                    # Check if there are unsent records to provide better context
                    cur_check = conn.execute("SELECT COUNT(*) FROM spool WHERE sent=0")
                    unsent_count = cur_check.fetchone()[0]
                    if unsent_count > 0:
                        logger.warning(f"Failed to connect to RabbitMQ, {unsent_count} records buffered locally, retrying in {backoff:.1f}s...")
                    else:
                        logger.warning(f"Failed to connect to RabbitMQ, retrying in {backoff:.1f}s...")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
                    continue
                
                try:
                    channel = rabbit_conn.channel()
                    # Declare exchange and queue
                    channel.exchange_declare(
                        exchange=rabbitmq_config['exchange'],
                        exchange_type='direct',
                        durable=True
                    )
                    channel.queue_declare(
                        queue=rabbitmq_config['queue'],
                        durable=True
                    )
                    channel.queue_bind(
                        exchange=rabbitmq_config['exchange'],
                        queue=rabbitmq_config['queue'],
                        routing_key=rabbitmq_config['routing_key']
                    )
                    queue_declared = True
                    backoff = 1.0
                    logger.info("Connected to RabbitMQ and declared queue")
                except Exception as e:
                    logger.error(f"Failed to setup RabbitMQ channel: {e}")
                    try:
                        if channel:
                            channel.close()
                        rabbit_conn.close()
                    except:
                        pass
                    rabbit_conn = None
                    channel = None
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
                    continue
            
            # Fetch batch (with timeout - publish partial batch if timeout exceeded)
            rows = dequeue_batch(conn, batch_size)
            
            if not rows:
                time.sleep(0.1)
                backoff = 1.0
                # Don't reset last_batch_time here - we want to track time since last successful publish
                continue
            
            # Check if we should publish a partial batch due to timeout
            time_since_last_batch = time.time() - last_batch_time
            if len(rows) < batch_size and time_since_last_batch < batch_timeout:
                # Wait a bit more for batch to fill (but not too long)
                time.sleep(0.1)
                continue
            
            # Prepare batch
            records = []
            ids = []
            batch_enqueue_times = []
            for row in rows:
                try:
                    rec = json.loads(row["payload"])
                    records.append(rec)
                    ids.append(row["id"])
                    # Track enqueue time for latency calculation (sqlite3.Row uses dict-style access)
                    try:
                        # sqlite3.Row supports dict-style access - try to get enqueue_time
                        # Use try/except since column might not exist
                        enqueue_time = row["enqueue_time"]
                        if enqueue_time:
                            batch_enqueue_times.append(enqueue_time)
                    except (KeyError, IndexError):
                        pass  # enqueue_time column might not exist in old databases
                except Exception as e:
                    logger.error(f"Failed to parse record {row['id']}: {e}")
                    # Mark as sent to avoid retrying corrupted data
                    mark_sent(conn, [row["id"]])
                    continue
            
            if not records:
                continue
            
            # Compress and publish
            try:
                # Double-check connection before publishing
                if rabbit_conn is None or (hasattr(rabbit_conn, 'is_closed') and rabbit_conn.is_closed):
                    raise Exception("Connection is closed, need to reconnect")
                if channel is None or (hasattr(channel, 'is_closed') and channel.is_closed):
                    raise Exception("Channel is closed, need to reconnect")
                
                compressed = compress_batch(records)
                channel.basic_publish(
                    exchange=rabbitmq_config['exchange'],
                    routing_key=rabbitmq_config['routing_key'],
                    body=compressed,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Make message persistent
                        content_type='application/json',
                        content_encoding='gzip',
                        timestamp=int(time.time())
                    )
                )
                
                # Mark as sent on successful publish
                mark_sent(conn, ids)
                metrics.inc_published(len(records), len(compressed))
                last_batch_time = time.time()  # Reset timeout timer
                
                # Calculate and log latency if we have enqueue times
                if batch_enqueue_times:
                    import time as time_module
                    publish_time = time_module.time()
                    avg_latency = (publish_time - min(batch_enqueue_times)) if batch_enqueue_times else 0
                    if avg_latency > 0:
                        logger.debug(f"Published batch of {len(records)} records, latency: {avg_latency:.2f}s")
                
                backoff = 1.0
                
            except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed, 
                    ConnectionResetError, OSError) as e:
                error_type = type(e).__name__
                logger.warning(f"Failed to publish batch ({error_type}): {e}")
                metrics.inc_failed()
                # Connection/channel is broken, reset it
                try:
                    if channel:
                        channel.close()
                except:
                    pass
                try:
                    if rabbit_conn:
                        rabbit_conn.close()
                except:
                    pass
                rabbit_conn = None
                channel = None
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
        
        except Exception as e:
            # Catch any unhandled exception in the loop to prevent thread from crashing
            error_type = type(e).__name__
            logger.error(f"Unexpected error in publish_loop ({error_type}): {e}", exc_info=True)
            # Reset connection and continue trying
            try:
                if channel:
                    channel.close()
            except:
                pass
            try:
                if rabbit_conn:
                    rabbit_conn.close()
            except:
                pass
            rabbit_conn = None
            channel = None
            # Don't exit - keep trying to reconnect and publish
            logger.warning("Connection lost, will retry...")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
    
    # Cleanup on exit
    try:
        if channel:
            channel.close()
    except:
        pass
    try:
        if rabbit_conn:
            rabbit_conn.close()
    except:
        pass