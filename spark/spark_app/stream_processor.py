#!/usr/bin/env python3
import io
import logging
import math
import os
import json
from typing import Dict, List
from datetime import datetime, time

import concurrent
import psycopg2
from psycopg2.extras import execute_values


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

from elasticsearch import Elasticsearch, helpers
from kafka import KafkaProducer
# ==========================
# CONFIG (edit via env when possible)
# ==========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_events")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "anomaly_alerts")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "dead_letter_topic")



POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/realtime_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "user_event_logs")


#timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S")  # Ví dụ: 20250810143501
#CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", f"/opt/spark/spark-checkpoints/{timestamp_suffix}")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/opt/spark/spark-checkpoints/realtime")
ANOMALY_THRESHOLD = int(os.getenv("ANOMALY_THRESHOLD", "5"))  # threshold for anomaly
COPY_CHUNK = int(os.getenv("COPY_CHUNK", "5000"))  # number of rows per COPY chunk to avoid huge memory spike
ES_BULK_CHUNK = int(os.getenv("ES_BULK_CHUNK", "2000"))

# Windows / triggers
PROCESSING_TRIGGER = os.getenv("PROCESSING_TRIGGER", "1 seconds")
WINDOW_DURATION_5S = "5 seconds"
WINDOW_DURATION_1MIN = "1 minute"
WINDOW_SLIDE = os.getenv("WINDOW_SLIDE", "5 seconds")
WATERMARK_5S = "30 seconds"
WATERMARK_1MIN = "2 minutes"
Z_THRESHOLD = float(os.getenv("Z_THRESHOLD", "2.0"))

# Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("stream_processor")

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

# ----------------------
# Elasticsearch helper
# ----------------------
_es_client = None
def get_es_client():
    global _es_client
    if _es_client is None:
        try:
            _es_client = Elasticsearch([ES_HOST], request_timeout=10)
        except Exception as e:
            logger.warning("ES client init failed: %s", e)
            _es_client = None
    return _es_client

# ----------------------
# Kafka producer helper (alerts & DLQ)
# ----------------------
_alert_producer = None
def get_alert_producer(max_retries=3, retry_delay=5):
    global _alert_producer
    if _alert_producer is None:
        for attempt in range(max_retries):
            try:
                _alert_producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    api_version=(2, 8, 0),
                    compression_type="snappy"
                )
                logger.info("Kafka producer initialized successfully")
                return _alert_producer
            except Exception as e:
                logger.warning("Kafka producer init failed (attempt %d/%d): %s", attempt + 1, max_retries, e)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        logger.error("Failed to initialize Kafka producer after %d attempts", max_retries)
        _alert_producer = None
    return _alert_producer
        

# ==========================
# Spark session
# ==========================
spark = SparkSession.builder \
    .appName("StreamProcessor") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.fileSink.logCleanupDelay", "1 hours") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

users_df = spark.read.jdbc(
    url=POSTGRES_URL, table="users",
    properties={"user": POSTGRES_USER, "password": POSTGRES_PASSWORD}
).cache()

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
    .dropna(subset=["user_id", "product_id", "action", "event_time"]) \
    .repartition("user_id")

# Print schema for debugging
logger.info("Parsed schema: %s", parsed.schema)


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

# Dead-letter: send chunk to DLQ
def send_chunk_to_dlq(chunk_rows: List, batch_id: int, reason: str, max_retries=3):
    prod = get_alert_producer()
    if not prod:
        logger.warning("No Kafka producer for DLQ; dropping %d rows", len(chunk_rows))
        with open("tmp/dlq.log", "a") as f:
            for r in chunk_rows:
                f.write(json.dumps({"reason": reason, "batch_id": batch_id, "row": r, "reported_at": datetime.utcnow().isoformat()}) + "\n")
        return
    if not chunk_rows:
        return
    logger.warning("Sending batch %d to DLQ: %s", batch_id, chunk_rows)
    for attempt in range(max_retries):
        try:
            for r in chunk_rows:
                msg = {"reason": reason, "batch_id": batch_id, "row": r, "reported_at": datetime.utcnow().isoformat()}
                prod.send(KAFKA_DLQ_TOPIC, msg)
            prod.flush(timeout=5)
            logger.info("Sent %d rows to DLQ", len(chunk_rows))
            return
        except Exception as e:
            logger.warning("DLQ attempt %d failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(1)  # wait before retrying
    logger.error("Failed to send %d rows to DLQ after %d attempts", len(chunk_rows), max_retries)

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
                        try:
                            cur.copy_from(buf, "user_events", sep="\t", null="\\N", columns=tuple(cols))
                            conn.commit()
                            sent += len(chunk)
                        except Exception as e:
                            conn.rollback()
                            logger.warning("Failed COPY-ing chunk to user_events: %s", e)
                            send_chunk_to_dlq(chunk, batch_id, str(e))
                    break
                chunk.append(row.asDict())
                if len(chunk) >= COPY_CHUNK:
                    buf = row_to_copy_buffer(chunk, cols)
                    try:
                        cur.copy_from(buf, "user_events", sep="\t", null="\\N", columns=tuple(cols))
                        conn.commit()
                        sent += len(chunk)
                    except Exception as e:
                        conn.rollback()
                        logger.warning("Failed COPY-ing chunk to user_events: %s", e)
                        send_chunk_to_dlq(chunk, batch_id, str(e))
                    chunk = []
            logger.info("[raw] Batch %s COPY-ed %d rows to user_events", batch_id, sent)
            es = get_es_client()
            if es:
                try:
                    es.index(index=ES_INDEX, document={"type": "raw_batch", "batch_id": batch_id, "records": sent, "logged_at": datetime.utcnow().isoformat()})
                except TypeError:
                    es.index(index=ES_INDEX, body={"type": "raw_batch", "batch_id": batch_id, "records": sent, "logged_at": datetime.utcnow().isoformat()})
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass  
    except Exception as e:
         logger.exception("[ERROR] write_raw_via_copy exception for batch %s: %s", batch_id, e)
# Start raw stream

raw_query = parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(foreach_write_raw) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/user_events") \
    .trigger(processingTime=PROCESSING_TRIGGER) \
    .start()
    
# ==========================
# Write Parquet
# ==========================
parquet_query = parsed.writeStream \
    .format("parquet") \
    .option("path", "/tmp/parquet-logs") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/parquet") \
    .trigger(processingTime=PROCESSING_TRIGGER) \
    .start()


# ==========================
# WINDOWED AGGREGATION -> summary + anomaly detection
# window: 1 minute, slide 30 seconds (short window as requested)
# watermark: 2 minutes
# ==========================
windowed_5s = parsed.withWatermark("event_time", WATERMARK_5S) \
    .groupBy(window(col("event_time"), WINDOW_DURATION_5S, WINDOW_SLIDE), col("user_id")) \
    .agg(count("*").alias("event_count"))
windowed_1min = parsed.withWatermark("event_time", WATERMARK_1MIN) \
    .groupBy(window(col("event_time"), WINDOW_DURATION_1MIN, WINDOW_SLIDE), col("user_id")) \
    .agg(count("*").alias("event_count"))


def write_summary_and_anomaly(batch_df, batch_id, window_type):
    """
    foreachBatch handler for aggregated windowed results.
    Writes summary into user_activity_summary via UPSERT, anomalies into anomalous_events (INSERT ON CONFLICT DO NOTHING),
    logs to ES and triggers Telegram alerts for anomalies.
    """
    try:
        if batch_df.rdd.isEmpty():
            logger.info("[agg] Batch %s (%s window) is empty -> skip", batch_id, window_type)
            return
        
   # materialize to local DF with proper columns
        summary_df = batch_df.coalesce(2).select(
            col("window").getField("start").alias("window_start"),
            col("window").getField("end").alias("window_end"),
            col("user_id"),
            col("event_count")
        )
        # convert to pandas or collect for psycopg2 upserts.
        enriched_df = summary_df.join(users_df, "user_id", "left") \
            .select(
                col("window_start"), col("window_end"), col("user_id"), col("event_count"),
                col("name").alias("user_name"), col("region")
            )
        rows = enriched_df.collect()

        if not rows:
            logger.info("[agg] Batch %s (%s window) has no summary rows -> skip", batch_id, window_type)
            return

        # Prepare data tuples for upsert
        upsert_tuples = []
        for r in rows:
            # window_start/end are TimestampType (py datetime)
            ws = r["window_start"]
            we = r["window_end"]
            uid = int(r["user_id"])
            cnt = int(r["event_count"])
            upsert_tuples.append((ws, we, uid, cnt))
        # Compute simple batch-level statistics for z-score detection
        counts = [t[3] for t in upsert_tuples]
        if counts:
            mean = float(sum(counts)) / len(counts)
            var = sum((x - mean) ** 2 for x in counts) / len(counts)
            stddev = math.sqrt(var)
        else:
            mean = 0.0
            stddev = 0.0
        z_threshold = float(os.getenv("Z_THRESHOLD", "3.0"))

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
            logger.info("[agg] Batch %s (%s window) upserted %d summaries", batch_id, window_type, len(upsert_tuples))
        except Exception as e:
            conn.rollback()
            logger.exception("[ERROR] Failed upserting summaries for batch %s: %s", batch_id, e)
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
        # Detect anomalies: event_count > ANOMALY_THRESHOLD AND z-score > z_threshold
        anomalies = []
        
        for i, (ws, we, uid, cnt) in enumerate(upsert_tuples):
            z = (cnt - mean) / stddev if stddev > 0 else None
            logger.debug("User %d: event_count=%d, z_score=%s", uid, cnt, z)
            is_anomaly = cnt > ANOMALY_THRESHOLD or (z is not None and z >= z_threshold)
            
            if is_anomaly:
                severity = "high" if z > 5 else "medium" if z > 3 else "low"
                if severity in ["high", "medium"]:
                    anomalies.append((uid, ws.isoformat(), we.isoformat(), cnt, float(z), severity, rows[i]["user_name"], rows[i]["region"]))

    # Persist anomalies (psycopg2) with ON CONFLICT DO NOTHING
        if anomalies:
            try:
                conn = get_psycopg2_conn()
                cur = conn.cursor()
                anomaly_db_tuples = [(uid, ws_iso, we_iso, cnt) for (uid, ws_iso, we_iso, cnt, z, severity, _, _) in anomalies]
                insert_anom_sql = """
                INSERT INTO anomalous_events (user_id, window_start, window_end, event_count)
                VALUES %s
                ON CONFLICT (user_id, window_start, window_end) DO NOTHING;
                """
            # convert iso strings back to timestamps via psycopg2 by passing them as strings
                execute_values(cur, insert_anom_sql, anomaly_db_tuples, page_size=200)

                conn.commit()
                logger.info("[anomaly] Batch %s wrote %d anomalies", batch_id, len(anomalies))
            except Exception as e:
                logger.exception("[ERROR] Failed writing anomalies for batch %s: %s", batch_id, e)
            finally:
                try:
                    cur.close()
                except:
                    pass
                try:
                    conn.close()
                except:
                    pass
        # ES bulk index anomalies
        try:
            es = get_es_client()
            if es and anomalies:
                docs = []
                for (uid, ws_iso, we_iso, cnt, z, user_name, region, severity) in anomalies:
                    docs.append({
                        "_index": ES_INDEX, 
                        "_source": {
                            "type": "anomaly", 
                            "user_id": uid, 
                            "event_count": cnt, 
                            "window_start": ws_iso, 
                            "window_end": we_iso, 
                            "z_score": z, 
                            "severity": severity, 
                            "user_name": user_name,
                            "region": region,
                            "detected_at": datetime.utcnow().isoformat()
                        }
                    })
                helpers.bulk(es, docs, chunk_size=ES_BULK_CHUNK, raise_on_error=False)
                logger.info("[ES] Bulk indexed %d anomalies", len(docs))
        except Exception as e:
            logger.exception("[ERROR] ES bulk index anomalies failed for batch %s: %s", batch_id, e)
        # Send alerts to Kafka alert topic (batch send & flush once)
        try:
            prod = get_alert_producer()
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                if prod is None:
                    logger.error("Kafka producer is not initialized, cannot send alerts")
                elif anomalies:
                    for (uid, ws_iso, we_iso, cnt, z, severity, user_name, region) in anomalies:
                        alert_msg = {
                            "user_id": uid,
                            "user_name": user_name,
                            "region": region,
                            "window_start": ws_iso,
                            "window_end": we_iso,
                            "event_count": cnt,
                            "z_score": z,
                            "severity": severity,
                            "detected_at": datetime.utcnow().isoformat()
                        }
                        print(f"[ALERT] {json.dumps(alert_msg)}")
                        executor.submit(lambda msg=alert_msg: open("/tmp/alerts.log", "a").write(json.dumps(msg) + "\n"))
                        try:
                            prod.send(KAFKA_ALERT_TOPIC, alert_msg)
                        except Exception as e:
                            logger.warning("[Kafka] Failed sending alert for anomaly: %s", e)
                    try:
                        prod.flush(timeout=5)
                        logger.info("[Kafka] Sent %d anomaly alerts to %s", len(anomalies), KAFKA_ALERT_TOPIC)
                    except Exception as e:
                        logger.warning("[Kafka] Failed flushing alerts: %s", e)
                else:
                    logger.info("[Kafka] No anomalies to send alerts")
            
        except Exception as e:
            logger.exception("[ERROR] Failed sending alerts to Kafka: %s", e)
        
        #Log batch summary to ES
        try:
            es = get_es_client()
            if es:
                summary_doc ={
                    "type": "agg_batch",
                    "batch_id": batch_id,
                    "window_type": window_type,
                    "records": len(upsert_tuples),
                    "anomalies": len(anomalies),
                    "mean_event_count": mean,
                    "stddev_event_count": stddev,
                    "z_threshold": z_threshold,
                    "logged_at": datetime.utcnow().isoformat()
                }
                try:
                    es.index(index=ES_INDEX, document=summary_doc)
                except TypeError:
                    es.index(index=ES_INDEX, body=summary_doc)
        except Exception as e:
            logger.exception("[ERROR] Failed logging agg batch summary to ES: %s", e)
    except Exception as e:
        logger.exception("[ERROR] write_summary_and_anomaly exception for batch %s: %s", batch_id, window_type, e)
# Start windowed stream
agg_query_5s = windowed_5s.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, id: write_summary_and_anomaly(df, id, "5s")) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/user_activity_summary_5s") \
    .trigger(processingTime=PROCESSING_TRIGGER) \
    .start()

agg_query_1min = windowed_1min.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, id: write_summary_and_anomaly(df, id, "1min")) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/user_activity_summary_1min") \
    .trigger(processingTime=PROCESSING_TRIGGER) \
    .start()

# ==========================
# Await termination
# ==========================
spark.streams.awaitAnyTermination()
