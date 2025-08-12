#!/usr/bin/env python3
import io
import os
import json
import traceback
from typing import Dict, List
import requests
from datetime import datetime
from urllib.parse import urlparse
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extras import execute_batch

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

from elasticsearch import Elasticsearch
from kafka import KafkaProducer
# ==========================
# CONFIG (edit via env when possible)
# ==========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_events")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "anomaly_alerts")


POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/realtime_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "user_event_logs")


timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S")  # Ví dụ: 20250810143501
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", f"/opt/spark/spark-checkpoints/{timestamp_suffix}")
ANOMALY_THRESHOLD = int(os.getenv("ANOMALY_THRESHOLD", "10"))  # threshold for anomaly
COPY_CHUNK = int(os.getenv("COPY_CHUNK", "5000"))  # number of rows per COPY chunk to avoid huge memory spike

# Windows / triggers
WINDOW_DURATION = "1 minute"
WINDOW_SLIDE = "30 seconds"
WATERMARK = "2 minutes"
PROCESSING_TRIGGER = "30 seconds"  # ~2 times per minute

# ==========================
# HELPERS: Postgres connection parsing & connect
# ==========================
def parse_jdbc_postgres(jdbc_url: str) -> Dict[str, str]:

    # Parse jdbc:postgresql://host:port/dbname (may include params)
    # returns dict with host, port, dbname

    # remove prefix
    prefix = "jdbc:postgresql://"
    if not jdbc_url.startswith(prefix):
        raise ValueError("Unsupported POSTGRES_URL format. Expect jdbc:postgresql://host:port/db")
    rest = jdbc_url[len(prefix):]
    # split host:port and db (db may contain ?params)
    if "/" not in rest:
        raise ValueError("POSTGRES_URL parsing failed")
    hostport, db_and_qs = rest.split("/", 1)
    if ":" in hostport:
        host, port = hostport.split(":", 1)
    else:
        host, port = hostport, "5432"
    # db could contain query string
    dbname = db_and_qs.split("?", 1)[0]
    return {"host": host, "port": int(port), "dbname": dbname}

PG_CONN_INFO = parse_jdbc_postgres(POSTGRES_URL)

def get_psycopg2_conn():
    params = PG_CONN_INFO
    conn = psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10
    )
    return conn

_alert_producer = None
def get_alert_producer():
    global _alert_producer
    if _alert_producer is None:
        try:
            _alert_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(2, 8, 0)  # Adjust based on your Kafka version
            )
            return _alert_producer
        except Exception as e:
            print(f"[WARN] Kafka producer init failed: {e}")
            _alert_producer = None
        
        
_es_client = None

def get_es_client():
    global _es_client
    if _es_client is None:
        try:
            _es_client = Elasticsearch([ES_HOST], timeout=10)
        except Exception as e:
            print(f"[WARN] ES client init failed: {e}")
            _es_client = None
    return _es_client

def es_index_record(doc: dict, index: str = ES_INDEX):
    try:
        es = get_es_client()
         # compatibility: elasticsearch-py >=8 uses 'document'; older uses 'body'
        try:
             es.index(index=index, document=doc)
        except TypeError:
            es.index(index=index, body=doc)
    except Exception as e:
        print(f"[WARN] ES index failed: {e}")

# ==========================
# Spark session
# ==========================
spark = SparkSession.builder \
    .appName("StreamProcessor") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ==========================
# Schema for incoming Kafka JSON
# ==========================
event_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("action", StringType(), False),
    StructField("timestamp", TimestampType(), False),  # iso string from producer
    StructField("device_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("user_segment", StringType(), True),
    StructField("ip_address", StringType(), True)
])

# ==========================
# Read from Kafka
# ==========================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

parsed = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), event_schema, {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"}).alias("data")) \
    .filter(col("data").isNotNull()) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp")) \
    .dropna(subset=["user_id", "product_id", "action", "event_time"])

# Print schema for debugging
print(parsed.schema)


# ==========================
# 1) Write raw events to Postgres (via JDBC) - drop event_time
# ==========================
def row_to_copy_buffer(row: List, columns: List[str]) -> io.StringIO:
    # Convert list of Row objects to a text buffer understood by COPY (tab-separated, \N for null).
    # Use timestamp formatted as "YYYY-MM-DD HH:MM:SS" to match TIMESTAMP without tz.
    buf = io.StringIO()
    for r in row:
        values = []
        for c in columns:
            val = r[c]
            if val is None:
                values.append(r"\N")
            else:
                # If it's TimestampType (python datetime), format appropriately
                if isinstance(val, datetime):
                    values.append(val.strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    # ensure no newlines or tabs break the COPY: replace \t and \n
                    s = str(val).replace("\t", " ").replace("\n", " ")
                    values.append(s)
        buf.write("\t".join(values) + "\n")
    buf.seek(0)
    return buf
    
def foreach_write_raw(batch_df, batch_id):

    # Bulk insert raw events via psycopg2 COPY FROM STDIN.
    # - batch_df: must include column event_time (TimestampType) and other fields.
    # - We drop event_time column when writing to user_events (table expects 'timestamp' column).

    try:
        if batch_df.rdd.isEmpty():
            print(f"[raw] Batch {batch_id} empty -> skip")
            return
        # Drop event_time before storing to user_events (schema does not include it)
        df_to_write = batch_df.drop("event_time")
        selected = df_to_write.select(
            col("user_id"),
            col("product_id"),
            col("action"),
            col("timestamp"),  
            col("device_id"),
            col("device_type"),
            col("location"),
            col("user_segment"),
            col("ip_address")
        )
        # Collect in reasonable chunks to avoid driver OOM
        count_est = df_to_write.count()
        es_index_record({
            "type": "raw_batch_prepare",
            "batch_id": batch_id,
            "estimated_records": int(count_est),
            "prepared_at": datetime.utcnow().isoformat()
        })
        
        # Convert partitions -> rows in chunks
        # Use .toLocalIterator() to avoid collecting all at once
        
        cols = ["user_id","product_id","action","timestamp","device_id","device_type","location","user_segment","ip_address"]
        it = selected.toLocalIterator()
        chunk = []
        sent = 0
        conn = get_psycopg2_conn()
        cur = conn.cursor()
        try:
            while True:
                try:
                    row = next(it)
                except StopIteration:
                    # flush last chunk
                    if chunk:
                        buf = row_to_copy_buffer(chunk, cols)
                        cur.copy_from(buf, "user_events", sep="\t", null="\\N", columns=tuple(cols))
                        sent += len(chunk)
                        chunk = []
                    break
                chunk.append(row.asDict())
                if len(chunk) >= COPY_CHUNK:
                    buf = row_to_copy_buffer(chunk, cols)
                    cur.copy_from(buf, "user_events", sep="\t", null="\\N", columns=tuple(cols))
                    sent += len(chunk)
                    chunk = []
            conn.commit()
            print(f"[raw] Batch {batch_id} COPY-ed {sent} rows to user_events")
            es_index_record({
                "type": "raw_batch",
                "batch_id": batch_id,
                "records": sent,
                "logged_at": datetime.utcnow().isoformat()
            })
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Failed writing raw batch {batch_id}: {e}")
            traceback.print_exc()
            raise
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        print(f"[ERROR] write_raw_via_copy exception for batch {batch_id}: {e}")
        traceback.print_exc()
# Start raw stream

raw_query = parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(foreach_write_raw) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/user_events") \
    .trigger(processingTime=PROCESSING_TRIGGER) \
    .start()

# ==========================
# WINDOWED AGGREGATION -> summary + anomaly detection
# window: 1 minute, slide 30 seconds (short window as requested)
# watermark: 2 minutes
# ==========================
windowed = parsed.withWatermark("event_time", WATERMARK) \
    .groupBy(window(col("event_time"), WINDOW_DURATION, WINDOW_SLIDE), col("user_id")) \
    .agg(count("*").alias("event_count"))


def write_summary_and_anomaly(batch_df, batch_id):
    """
    foreachBatch handler for aggregated windowed results.
    Writes summary into user_activity_summary via UPSERT, anomalies into anomalous_events (INSERT ON CONFLICT DO NOTHING),
    logs to ES and triggers Telegram alerts for anomalies.
    """
    try:
        if batch_df.rdd.isEmpty():
            print(f"[summary] Batch {batch_id} empty -> skipping")
            return
        
   # materialize to local DF with proper columns
        summary_df = batch_df.select(
            col("window").getField("start").alias("window_start"),
            col("window").getField("end").alias("window_end"),
            col("user_id"),
            col("event_count")
        )
        # convert to pandas or collect for psycopg2 upserts.
        rows = summary_df.collect()

        if not rows:
            print(f"[agg] Batch {batch_id} no rows -> skip")
            return

        # Prepare data tuples for upsert
        upsert_tuples = []
        for r in rows:
            # window_start/end are TimestampType (py datetime)
            ws = r["window_start"]  # Timestamp
            we = r["window_end"]
            uid = int(r["user_id"])
            cnt = int(r["event_count"])
            upsert_tuples.append((ws, we, uid, cnt))
        # Compute simple batch-level statistics for z-score detection
        counts = [t[3] for t in upsert_tuples]
        mean = float(sum(counts)) / len(counts) if counts else 0.0
        stddev = float(sum((x - mean) ** 2 for x in counts)) / len(counts) ** 0.5 if counts else 0.0
        z_threshold = 2.0

        # Connect to Postgres and upsert
        conn = None
        try:
            conn = get_psycopg2_conn()
            cur = conn.cursor()

            # Upsert into user_activity_summary:
            # window_start TIMESTAMP, window_end TIMESTAMP, user_id, event_count
            upsert_sql = """
            INSERT INTO user_activity_summary (window_start, window_end, user_id, event_count)
            VALUES %s
            ON CONFLICT (user_id, window_start, window_end)
            DO UPDATE SET event_count = EXCLUDED.event_count;
            """
            execute_values(cur, upsert_sql, upsert_tuples, page_size=200)
            conn.commit()
            print(f"[agg] Batch {batch_id} upserted {len(upsert_tuples)} summary rows")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Failed upserting summaries: {e}")
            traceback.print_exc()
        finally:
            try:
                cur.close()
            except:
                pass
            conn.close()
        # Detect anomalies: event_count > ANOMALY_THRESHOLD AND z-score > z_threshold
        anomalies = []
        
        for (ws, we, uid, cnt) in upsert_tuples:
            z = None
            if stddev > 0:
                z = (cnt - mean) / stddev
            is_anomaly = cnt > ANOMALY_THRESHOLD and (z is not None and z >= z_threshold)
            
            if is_anomaly:
                anomalies.append((uid, ws.isoformat(), we.isoformat(), cnt, float(z) if z is not None else None))
            
    # Persist anomalies (psycopg2) with ON CONFLICT DO NOTHING
        if anomalies:
            try:
                conn = get_psycopg2_conn()
                cur = conn.cursor()
                insert_anom_sql = """
                INSERT INTO anomalous_events (user_id, window_start, window_end, event_count)
                VALUES %s
                ON CONFLICT (user_id, window_start, window_end) DO NOTHING;
                """
            # convert iso strings back to timestamps via psycopg2 by passing them as strings
                execute_values(cur, insert_anom_sql, [(uid, ws_iso, we_iso, cnt) for (uid, ws_iso, we_iso, cnt, z) in anomalies], page_size=200)

                conn.commit()
                cur.close()
                conn.close()
                print(f"[anomaly] Batch {batch_id} wrote {len(anomalies)} anomalies")
            except Exception as e:
                print(f"[WARN] Failed writing anomalies to DB: {e}")
                traceback.print_exc()

            
            # Log & alert per anomaly
            for an in anomalies:
                uid, ws_iso, we_iso, cnt, z = an
                alert_text = f"⚠️ Anomaly detected: user_id={uid}, count={cnt}, window={ws_iso} - {we_iso}, z={z}"
                print(f"[ALERT] {alert_text}")
                # best-effort non-blocking alert
                # Gửi vào Kafka alert topic
            try:
                producer = get_alert_producer()
                producer.send(KAFKA_ALERT_TOPIC, {
                    "user_id": uid,
                    "count": cnt,
                    "window_start": ws_iso,
                    "window_end": we_iso,
                    "z_score": z,
                    "detected_at": datetime.utcnow().isoformat()
                })
                producer.flush()
                print(f"[ALERT->KAFKA] Sent anomaly for user_id={uid} to topic={KAFKA_ALERT_TOPIC}")
            except Exception as e:
                print(f"[WARN] Failed to send anomaly alert to Kafka: {e}")

            # ES logging
                try:
                    es_doc = {
                        "type": "anomaly",
                        "user_id": uid,
                        "event_count": cnt,
                        "window_start": ws_iso,
                        "window_end": we_iso,
                        "z_score": z,
                        "detected_at": datetime.utcnow().isoformat()
                    }
                    es_index_record(es_doc)
                except Exception as e:
                    print(f"[WARN] ES logging anomaly failed: {e}")
    # Log batch summary to ES
        try:
            es_doc = {   
                "type": "aggregate_batch",
                "batch_id": batch_id,
                "records": len(upsert_tuples),
                "mean": mean,
                "stddev": stddev,
                "anomalies": len(anomalies),
                "logged_at": datetime.utcnow().isoformat()
            }
            es_index_record(es_doc)
        except Exception as e:
            print(f"[WARN] Telegram notify failed: {e}")
    except Exception as e:
        print(f"[ERROR] write_summary_and_anomalies failed for batch {batch_id}: {e}")
        traceback.print_exc()
# Start windowed stream
agg_query = windowed.writeStream \
    .outputMode("update") \
    .foreachBatch(write_summary_and_anomaly) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/user_activity_summary") \
    .trigger(processingTime=PROCESSING_TRIGGER) \
    .start()

# ==========================
# Await termination
# ==========================
spark.streams.awaitAnyTermination()
