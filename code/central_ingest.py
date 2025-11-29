#!/usr/bin/env python3
"""
Central Ingest Service - Consumes telemetry from RabbitMQ and stores in SQLite.

Features:
- Consumes batches from RabbitMQ queue
- Validates schema with Pydantic
- Deduplicates records (panel_id + timestamp_utc)
- Stores in SQLite with WAL mode
- Metrics logging for monitoring
- Graceful shutdown handling
"""
import gzip
import json
import logging
import signal
import sqlite3
import sys
import time
import threading
from datetime import datetime
from threading import Event
from typing import List, Optional

import pika
from pydantic import BaseModel, Field, ValidationError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

DB = "telemetry.sqlite"

DDL = """
CREATE TABLE IF NOT EXISTS readings(
  panel_id TEXT NOT NULL,
  timestamp_utc TEXT NOT NULL,
  payload TEXT NOT NULL,
  ingested_at REAL NOT NULL DEFAULT (julianday('now')),
  PRIMARY KEY(panel_id, timestamp_utc)
);
CREATE INDEX IF NOT EXISTS idx_ingested ON readings(ingested_at);
"""

class Record(BaseModel):
    timestamp_utc: str
    panel_id: str
    string_id: Optional[str] = None
    status: Optional[str] = None
    fault: Optional[str] = None
    power_w: Optional[float] = None
    voltage_v: Optional[float] = None
    current_a: Optional[float] = None
    irradiance_wm2: Optional[float] = None
    ambient_temp_c: Optional[float] = None
    cell_temp_c: Optional[float] = None
    orientation_deg: Optional[float] = None
    tilt_deg: Optional[float] = None

class Batch(BaseModel):
    records: List[Record] = Field(default_factory=list)

# Metrics
class Metrics:
    def __init__(self):
        self.records_received = 0
        self.records_accepted = 0
        self.records_rejected = 0
        self.records_duplicate = 0
        self.batches_processed = 0
        self.bytes_received = 0
        self.start_time = time.time()
        self.last_batch_time = time.time()
        self.processing_times = []  # Track actual processing latency per batch
    
    def inc_received(self, count: int, bytes_received: int, processing_time: float = 0):
        self.records_received += count
        self.batches_processed += 1
        self.bytes_received += bytes_received
        self.last_batch_time = time.time()
        if processing_time > 0:
            self.processing_times.append(processing_time)
            # Keep only last 100 measurements
            if len(self.processing_times) > 100:
                self.processing_times = self.processing_times[-100:]
    
    def inc_accepted(self, count: int):
        self.records_accepted += count
    
    def inc_rejected(self, count: int):
        self.records_rejected += count
    
    def inc_duplicate(self, count: int):
        self.records_duplicate += count
    
    def get_latency(self) -> float:
        """Calculate average processing latency (actual batch processing time)."""
        if self.processing_times:
            return sum(self.processing_times) / len(self.processing_times)
        return 0.0
    
    def get_time_since_last_batch(self) -> float:
        """Time since last batch was received."""
        return time.time() - self.last_batch_time
    
    def log_stats(self):
        elapsed = time.time() - self.start_time
        if elapsed < 1:
            return
        
        recv_rate = self.records_received / elapsed * 60  # per minute
        accept_rate = self.records_accepted / elapsed * 60  # per minute
        throughput_mbps = (self.bytes_received * 8) / elapsed / 1_000_000  # Mbps
        
        avg_processing_latency = self.get_latency()
        time_since_last = self.get_time_since_last_batch()
        
        logger.info(
            f"Metrics: Received={self.records_received} ({recv_rate:.0f}/min), "
            f"Accepted={self.records_accepted} ({accept_rate:.0f}/min), "
            f"Rejected={self.records_rejected}, Duplicates={self.records_duplicate}, "
            f"Batches={self.batches_processed}, Throughput={throughput_mbps:.2f} Mbps, "
            f"Processing Latency={avg_processing_latency:.3f}s, "
            f"Time Since Last={time_since_last:.1f}s"
        )

metrics = Metrics()

def open_db():
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    # Execute DDL statements one at a time
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()
    return conn

def decompress_batch(data: bytes) -> dict:
    """Decompress gzip batch."""
    try:
        decompressed = gzip.decompress(data)
        return json.loads(decompressed.decode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to decompress batch: {e}")
        raise

def process_batch(conn, batch_data: dict, bytes_received: int) -> tuple[int, int, int]:
    """Process a batch of records: validate, dedupe, and store."""
    import time as time_module
    from pydantic import ValidationError
    
    start_time = time_module.time()
    
    accepted = 0
    rejected = 0
    duplicate = 0
    
    # Get records list (handle both Batch model and raw dict)
    records_list = batch_data.get('records', [])
    if not records_list:
        processing_time = time_module.time() - start_time
        metrics.inc_received(0, bytes_received, processing_time)
        return 0, 0, 0
    
    processing_time = time_module.time() - start_time
    metrics.inc_received(len(records_list), bytes_received, processing_time)
    
    # Validate each record individually (so good records aren't affected by bad ones)
    for rec_data in records_list:
        try:
            # Validate individual record schema
            rec = Record(**rec_data)
            
            # Try to insert (deduplication via PRIMARY KEY)
            conn.execute(
                "INSERT OR IGNORE INTO readings(panel_id, timestamp_utc, payload) VALUES (?, ?, ?)",
                (rec.panel_id, rec.timestamp_utc, rec.model_dump_json())
            )
            
            # Check if actually inserted
            cur = conn.execute("SELECT changes()")
            if cur.fetchone()[0] > 0:
                accepted += 1
            else:
                duplicate += 1
                
        except ValidationError as e:
            # Schema validation failed - log the error with reason
            rec_id = rec_data.get('panel_id', 'unknown')
            rec_ts = rec_data.get('timestamp_utc', 'unknown')
            logger.error(f"Schema validation failed for record {rec_id}/{rec_ts}: {e}")
            rejected += 1
        except Exception as e:
            # Other errors (e.g., database errors)
            rec_id = rec_data.get('panel_id', 'unknown') if isinstance(rec_data, dict) else 'unknown'
            rec_ts = rec_data.get('timestamp_utc', 'unknown') if isinstance(rec_data, dict) else 'unknown'
            logger.error(f"Failed to store record {rec_id}/{rec_ts}: {e}")
            rejected += 1
    
    conn.commit()
    return accepted, rejected, duplicate

def create_rabbitmq_connection(host: str, port: int, username: str, password: str) -> Optional[pika.BlockingConnection]:
    """Create RabbitMQ connection."""
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

def consume_loop(conn, rabbitmq_config: dict, stop_event):
    """Main consumption loop."""
    rabbit_conn = None
    channel = None
    
    while not stop_event.is_set():
        # Ensure connection
        if rabbit_conn is None or rabbit_conn.is_closed:
            rabbit_conn = create_rabbitmq_connection(
                rabbitmq_config['host'],
                rabbitmq_config['port'],
                rabbitmq_config['username'],
                rabbitmq_config['password']
            )
            if rabbit_conn is None:
                time.sleep(5)
                continue
            
            try:
                channel = rabbit_conn.channel()
                # Set QoS to process one message at a time (for fairness)
                channel.basic_qos(prefetch_count=1)
                
                # Declare queue (in case it doesn't exist)
                channel.queue_declare(
                    queue=rabbitmq_config['queue'],
                    durable=True
                )
                
                # Purge queue if requested (for clean test runs)
                if rabbitmq_config.get('purge_on_start', False):
                    purged = channel.queue_purge(queue=rabbitmq_config['queue'])
                    logger.info(f"Purged {purged} messages from queue")
                
                logger.info("Connected to RabbitMQ and ready to consume")
            except Exception as e:
                logger.error(f"Failed to setup RabbitMQ channel: {e}")
                rabbit_conn.close()
                rabbit_conn = None
                time.sleep(5)
                continue
        
        # Consume messages
        try:
            method_frame, header_frame, body = channel.basic_get(
                queue=rabbitmq_config['queue'],
                auto_ack=False
            )
            
            if method_frame is None:
                # No message available
                time.sleep(0.1)
                continue
            
            # Process message
            try:
                batch_data = decompress_batch(body)
                accepted, rejected, duplicate = process_batch(conn, batch_data, len(body))
                
                metrics.inc_accepted(accepted)
                metrics.inc_rejected(rejected)
                metrics.inc_duplicate(duplicate)
                
                # Acknowledge message
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                
                logger.debug(
                    f"Processed batch: accepted={accepted}, rejected={rejected}, "
                    f"duplicate={duplicate}"
                )
                
            except Exception as e:
                logger.error(f"Failed to process message: {e}")
                # Reject and requeue (will retry)
                channel.basic_nack(
                    delivery_tag=method_frame.delivery_tag,
                    requeue=True
                )
        
        except pika.exceptions.AMQPConnectionError:
            logger.warning("Connection lost, reconnecting...")
            try:
                if channel:
                    channel.close()
                if rabbit_conn:
                    rabbit_conn.close()
            except:
                pass
            rabbit_conn = None
            channel = None
            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error in consume loop: {e}")
            time.sleep(1)
    
    # Cleanup
    try:
        if channel:
            channel.close()
        if rabbit_conn:
            rabbit_conn.close()
    except:
        pass

def metrics_logger_loop(stop_event):
    """Periodically log metrics."""
    while not stop_event.is_set():
        time.sleep(10)  # Log every 10 seconds
        metrics.log_stats()

def main():
    import argparse
    
    ap = argparse.ArgumentParser(description="Central ingest service with RabbitMQ Pub/Sub")
    ap.add_argument("--db", default="telemetry.sqlite", help="SQLite database path")
    ap.add_argument("--rabbitmq-host", default="localhost", help="RabbitMQ host")
    ap.add_argument("--rabbitmq-port", type=int, default=5672, help="RabbitMQ port")
    ap.add_argument("--rabbitmq-username", default="guest", help="RabbitMQ username")
    ap.add_argument("--rabbitmq-password", default="guest", help="RabbitMQ password")
    ap.add_argument("--rabbitmq-queue", default="telemetry_queue", help="RabbitMQ queue name")
    ap.add_argument("--purge-queue", action="store_true", help="Purge queue on startup (for clean test runs)")
    args = ap.parse_args()
    
    global DB
    DB = args.db
    
    logger.info("Starting central ingest service...")
    logger.info(f"Database: {DB}")
    logger.info(f"RabbitMQ: {args.rabbitmq_host}:{args.rabbitmq_port}")
    
    conn = open_db()
    stop_event = Event()
    
    rabbitmq_config = {
        'host': args.rabbitmq_host,
        'port': args.rabbitmq_port,
        'username': args.rabbitmq_username,
        'password': args.rabbitmq_password,
        'queue': args.rabbitmq_queue,
        'purge_on_start': args.purge_queue
    }
    
    # Handle graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal, stopping...")
        stop_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start metrics logger
    metrics_thread = threading.Thread(target=metrics_logger_loop, args=(stop_event,), daemon=True)
    metrics_thread.start()
    
    # Start consuming
    try:
        consume_loop(conn, rabbitmq_config, stop_event)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        stop_event.set()
        logger.info("Waiting for threads to finish...")
        metrics_thread.join(timeout=2)
        conn.close()
        metrics.log_stats()
        logger.info("Central ingest service stopped")

if __name__ == "__main__":
    main()
