import json
import logging
from datetime import datetime, timezone

from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer
)
from pyflink.datastream.functions import MapFunction, FilterFunction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Job1-GPS")

KAFKA_BROKERS  = "172.18.0.2:9092"
KAFKA_TOPIC    = "raw.gps"
KAFKA_GROUP    = "flink-job1-gps"
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
CITY           = "casablanca"

LAT_MIN, LAT_MAX = 33.4, 33.7
LON_MIN, LON_MAX = -7.8, -7.4
MAX_SPEED_KMH    = 150.0

ZONE_CENTROIDS = {
    1:  (33.5731, -7.5898),  2:  (33.5892, -7.6114),
    3:  (33.5650, -7.6200),  4:  (33.5480, -7.5750),
    5:  (33.5320, -7.6050),  6:  (33.5950, -7.5600),
    7:  (33.5820, -7.5400),  8:  (33.5600, -7.5900),
    9:  (33.5420, -7.6300),  10: (33.5750, -7.6400),
    11: (33.5580, -7.5580),  12: (33.5900, -7.5800),
    13: (33.5670, -7.6600),  14: (33.5450, -7.5450),
    15: (33.5300, -7.5800),  16: (33.5800, -7.5200),
}

def assign_zone(lat, lon):
    best_zone, best_dist = 1, float('inf')
    for zid, (clat, clon) in ZONE_CENTROIDS.items():
        d = (lat - clat)**2 + (lon - clon)**2
        if d < best_dist:
            best_dist, best_zone = d, zid
    return best_zone


class ParseAndValidateGPS(MapFunction):
    def map(self, raw):
        try:
            msg   = json.loads(raw)
            lat   = float(msg["lat"])
            lon   = float(msg["lon"])
            speed = float(msg.get("speed", 0.0))
            if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
                return None
            if speed > MAX_SPEED_KMH:
                return None
            zone_id = assign_zone(lat, lon)
            clat, clon = ZONE_CENTROIDS[zone_id]
            return json.dumps({
                "city": CITY, "zone_id": zone_id,
                "taxi_id": str(msg["taxi_id"]),
                "event_time": int(msg.get("timestamp", 0)),
                "lat": clat, "lon": clon,
                "speed": speed,
                "status": str(msg.get("status", "available")),
            })
        except Exception as ex:
            logger.debug(f"Ignored: {ex}")
            return None


class FilterValid(FilterFunction):
    def filter(self, v):
        return v is not None


class WriteToCassandra(MapFunction):
    def __init__(self):
        self._session = None
        self._stmt    = None

    def _connect(self):
        if self._session is None:
            from cassandra.cluster import Cluster
            from cassandra.policies import DCAwareRoundRobinPolicy
            cluster = Cluster(
                [CASSANDRA_HOST], port=CASSANDRA_PORT,
                load_balancing_policy=DCAwareRoundRobinPolicy(
                    local_dc='datacenter1')
            )
            self._session = cluster.connect("taasim")
            self._stmt = self._session.prepare("""
                INSERT INTO vehicle_positions
                  (city, zone_id, event_time, taxi_id,
                   lat, lon, speed, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """)
            logger.info("[Job1] Cassandra OK")

    def map(self, json_str):
        try:
            self._connect()
            e  = json.loads(json_str)
            ts = datetime.fromtimestamp(
                e["event_time"]/1000, tz=timezone.utc)
            self._session.execute(self._stmt, (
                e["city"], e["zone_id"], ts, e["taxi_id"],
                e["lat"], e["lon"], e["speed"], e["status"],
            ))
            logger.info(
                f"[Job1] Zone {e['zone_id']} | Taxi {e['taxi_id']}")
        except Exception as ex:
            logger.error(f"[Job1] Cassandra error: {ex}")
        return json_str


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.enable_checkpointing(60_000)
    env.get_checkpoint_config().set_checkpoint_storage_dir(
        "file:///tmp/flink-checkpoints/job1"
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(3))
        .with_idleness(Duration.of_seconds(10))
    )

    (
        env.from_source(source, watermark, "Kafka-raw.gps")
           .map(ParseAndValidateGPS(), output_type=Types.STRING())
           .filter(FilterValid())
           .map(WriteToCassandra(),   output_type=Types.STRING())
           .print()
    )

    env.execute("TaaSim - Job1 - GPS Normalizer")


if __name__ == "__main__":
    main()
