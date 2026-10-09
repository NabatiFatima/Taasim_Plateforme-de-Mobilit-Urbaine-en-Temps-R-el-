"""
JOB 2 — Demand Aggregator
==========================
processed.gps + raw.trips → fenêtre tumbling 30s par zone
→ Cassandra demand_zones
- Compte véhicules actifs + demandes en attente
- Calcule ratio supply/demand
"""
import json, logging
from datetime import datetime, timezone
from pyflink.common import WatermarkStrategy, Duration, Time, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction, FilterFunction, WindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Job2")

KAFKA_BROKERS  = "172.18.0.2:9092"
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
CITY           = "casablanca"

ZONE_CENTROIDS = {
    1:(33.5731,-7.5898), 2:(33.5892,-7.6114), 3:(33.5650,-7.6200),
    4:(33.5480,-7.5750), 5:(33.5320,-7.6050), 6:(33.5950,-7.5600),
    7:(33.5820,-7.5400), 8:(33.5600,-7.5900), 9:(33.5420,-7.6300),
    10:(33.5750,-7.6400),11:(33.5580,-7.5580),12:(33.5900,-7.5800),
    13:(33.5670,-7.6600),14:(33.5450,-7.5450),15:(33.5300,-7.5800),
    16:(33.5800,-7.5200),
}

def assign_zone(lat, lon):
    best, dist = 1, float('inf')
    for zid,(clat,clon) in ZONE_CENTROIDS.items():
        d = (lat-clat)**2 + (lon-clon)**2
        if d < dist:
            dist, best = d, zid
    return best

class ParseGPSEvent(MapFunction):
    def map(self, raw):
        try:
            m = json.loads(raw)
            ts = int(m.get("timestamp", 0))
            if ts < 1_000_000_000_000:
                ts *= 1000
            zone_id = m.get("zone_id") or assign_zone(float(m["lat"]), float(m["lon"]))
            return json.dumps({
                "type": "gps", "zone_id": int(zone_id),
                "taxi_id": str(m["taxi_id"]),
                "event_time": ts,
                "status": str(m.get("status","available")),
            })
        except Exception as e:
            logger.debug(f"GPS parse error: {e}")
            return None

class ParseTripEvent(MapFunction):
    def map(self, raw):
        try:
            m = json.loads(raw)
            ts = int(m.get("requested_at", m.get("timestamp", 0)))
            if ts < 1_000_000_000_000:
                ts *= 1000
            return json.dumps({
                "type": "trip", "zone_id": int(m.get("origin_zone", 1)),
                "trip_id": str(m.get("trip_id","")),
                "event_time": ts,
            })
        except Exception as e:
            logger.debug(f"Trip parse error: {e}")
            return None

class FilterValid(FilterFunction):
    def filter(self, v):
        return v is not None

class DemandWindowFunction(WindowFunction):
    def apply(self, zone_id, window, events, collector):
        active_vehicles  = set()
        pending_requests = 0
        for e in events:
            ev = json.loads(e)
            if ev["type"] == "gps":
                active_vehicles.add(ev["taxi_id"])
            elif ev["type"] == "trip":
                pending_requests += 1
        collector.collect(json.dumps({
            "zone_id":          zone_id,
            "window_start":     window.start,
            "active_vehicles":  len(active_vehicles),
            "pending_requests": pending_requests,
            "ratio":            pending_requests / max(len(active_vehicles), 1),
        }))

class WriteCassandra(MapFunction):
    def __init__(self):
        self._session = None
        self._stmt = None

    def _connect(self):
        if self._session is None:
            from cassandra.cluster import Cluster
            from cassandra.policies import DCAwareRoundRobinPolicy
            cluster = Cluster(
                [CASSANDRA_HOST], port=CASSANDRA_PORT,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
                protocol_version=5
            )
            self._session = cluster.connect("taasim")
            self._stmt = self._session.prepare("""
                INSERT INTO demand_zones
                  (city,zone_id,window_start,active_vehicles,
                   pending_requests,ratio,forecast_demand)
                VALUES (?,?,?,?,?,?,?)
            """)
            logger.info("[Job2] Cassandra connecte")

    def map(self, json_str):
        try:
            self._connect()
            e = json.loads(json_str)
            ws = datetime.fromtimestamp(e["window_start"]/1000, tz=timezone.utc)
            self._session.execute(self._stmt,(
                CITY, e["zone_id"], ws,
                e["active_vehicles"], e["pending_requests"],
                e["ratio"], 0.0,
            ))
            logger.info(
                f"[Job2] Zone {e['zone_id']} | "
                f"Vehicules={e['active_vehicles']} | "
                f"Demandes={e['pending_requests']} | "
                f"Ratio={e['ratio']:.2f}"
            )
        except Exception as ex:
            logger.error(f"[Job2] Erreur: {ex}")
        return json_str

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.enable_checkpointing(60_000)
    env.get_checkpoint_config().set_checkpoint_storage_dir("file:///tmp/flink-checkpoints/job2")

    watermark = (WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(3))
        .with_idleness(Duration.of_seconds(10)))

    # Source GPS (processed.gps si disponible, sinon raw.gps)
    gps_source = (KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.gps")
        .set_group_id("flink-job2-gps-v1")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())

    # Source Trips
    trips_source = (KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.trips")
        .set_group_id("flink-job2-trips-v1")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())

    gps_stream = (env.from_source(gps_source, watermark, "Kafka-raw.gps")
        .map(ParseGPSEvent(), output_type=Types.STRING())
        .filter(FilterValid()))

    trips_stream = (env.from_source(trips_source, watermark, "Kafka-raw.trips")
        .map(ParseTripEvent(), output_type=Types.STRING())
        .filter(FilterValid()))

    # Union → key par zone → fenêtre 30s
    (gps_stream.union(trips_stream)
        .key_by(lambda e: json.loads(e)["zone_id"])
        .window(TumblingEventTimeWindows.of(Time.seconds(30)))
        .apply(DemandWindowFunction())
        .map(WriteCassandra(), output_type=Types.STRING())
        .print())

    env.execute("TaaSim - Job2 - Demand Aggregator")

if __name__ == "__main__":
    main()
