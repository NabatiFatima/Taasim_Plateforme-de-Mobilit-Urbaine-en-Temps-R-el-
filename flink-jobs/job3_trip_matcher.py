"""
JOB 3 — Trip Matcher
=====================
raw.trips + processed.gps → matching → Cassandra trips
- State Flink : véhicules disponibles par zone (RocksDB)
- SLA : match < 5 secondes
- Fallback : zones adjacentes si pas de véhicule
- Idempotence : IF NOT EXISTS
"""
import json, logging, uuid, math
from datetime import datetime, timezone
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, FilterFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.common.typeinfo import Types as T

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Job3")

KAFKA_BROKERS  = "172.18.0.2:9092"
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
CITY           = "casablanca"
AVG_SPEED_KMH  = 30.0

ZONE_CENTROIDS = {
    1:(33.5731,-7.5898), 2:(33.5892,-7.6114), 3:(33.5650,-7.6200),
    4:(33.5480,-7.5750), 5:(33.5320,-7.6050), 6:(33.5950,-7.5600),
    7:(33.5820,-7.5400), 8:(33.5600,-7.5900), 9:(33.5420,-7.6300),
    10:(33.5750,-7.6400),11:(33.5580,-7.5580),12:(33.5900,-7.5800),
    13:(33.5670,-7.6600),14:(33.5450,-7.5450),15:(33.5300,-7.5800),
    16:(33.5800,-7.5200),
}

ADJACENT = {
    1:[2,6,10], 2:[1,3,9], 3:[2,4,13], 4:[3,5,14], 5:[4,9,15],
    6:[1,7,12], 7:[6,8,11], 8:[7,9,10], 9:[2,5,8], 10:[1,8,11],
    11:[7,10,14],12:[6,13,16],13:[3,12,14],14:[4,11,13,15],
    15:[5,14,16],16:[12,15],
}

def assign_zone(lat, lon):
    best, dist = 1, float('inf')
    for zid,(clat,clon) in ZONE_CENTROIDS.items():
        d = (lat-clat)**2 + (lon-clon)**2
        if d < dist:
            dist, best = d, zid
    return best

def eta_seconds(zone1, zone2):
    c1 = ZONE_CENTROIDS.get(zone1,(33.57,-7.59))
    c2 = ZONE_CENTROIDS.get(zone2,(33.57,-7.59))
    R = 6371
    dlat = math.radians(c2[0]-c1[0])
    dlon = math.radians(c2[1]-c1[1])
    a = math.sin(dlat/2)**2 + math.cos(math.radians(c1[0]))*math.cos(math.radians(c2[0]))*math.sin(dlon/2)**2
    dist = R * 2 * math.asin(math.sqrt(a))
    return int((dist / AVG_SPEED_KMH) * 3600)

class ParseEvent(MapFunction):
    def map(self, raw):
        try:
            m = json.loads(raw)
            # Déterminer le type d'event
            if "origin_zone" in m or "rider_id" in m:
                ts = int(m.get("requested_at", m.get("timestamp", 0)))
                if ts < 1_000_000_000_000: ts *= 1000
                return json.dumps({
                    "type": "trip",
                    "zone_id": int(m.get("origin_zone", 1)),
                    "trip_id": str(m.get("trip_id", str(uuid.uuid4()))),
                    "rider_id": str(m.get("rider_id","unknown")),
                    "dest_zone": int(m.get("destination_zone", m.get("dest_zone", 1))),
                    "event_time": ts,
                })
            else:
                ts = int(m.get("timestamp", 0))
                if ts < 1_000_000_000_000: ts *= 1000
                zone_id = m.get("zone_id") or assign_zone(float(m["lat"]), float(m["lon"]))
                return json.dumps({
                    "type": "gps",
                    "zone_id": int(zone_id),
                    "taxi_id": str(m["taxi_id"]),
                    "event_time": ts,
                    "status": str(m.get("status","available")),
                })
        except Exception as e:
            logger.debug(f"Parse error: {e}")
            return None

class FilterValid(FilterFunction):
    def filter(self, v):
        return v is not None

class TripMatcher(KeyedProcessFunction):
    def open(self, ctx):
        self._vehicles = ctx.get_map_state(MapStateDescriptor(
            "vehicles", T.STRING(), T.STRING()
        ))

    def process_element(self, value, ctx, out):
        e = json.loads(raw)
        if e["type"] == "gps":
            self._vehicles.put(e["taxi_id"], json.dumps({
                "zone_id": e["zone_id"],
                "last_seen": e["event_time"],
                "status": e["status"],
            }))
        elif e["type"] == "trip":
            origin = e["zone_id"]
            match = self._find_vehicle(origin)
            if match:
                taxi_id, taxi_zone = match
                # Marquer assigné
                v = json.loads(self._vehicles.get(taxi_id))
                v["status"] = "assigned"
                self._vehicles.put(taxi_id, json.dumps(v))
                eta = eta_seconds(origin, taxi_zone)
                out.collect(json.dumps({
                    "trip_id":     e["trip_id"],
                    "rider_id":    e["rider_id"],
                    "taxi_id":     taxi_id,
                    "origin_zone": origin,
                    "dest_zone":   e.get("dest_zone", origin),
                    "eta_seconds": eta,
                    "status":      "matched",
                    "event_time":  e["event_time"],
                }))
                logger.info(f"[Job3] MATCH trip {e['trip_id']} → {taxi_id} ETA={eta}s")
            else:
                out.collect(json.dumps({
                    "trip_id":     e["trip_id"],
                    "rider_id":    e["rider_id"],
                    "taxi_id":     "UNMATCHED",
                    "origin_zone": origin,
                    "dest_zone":   e.get("dest_zone", origin),
                    "eta_seconds": -1,
                    "status":      "unmatched",
                    "event_time":  e["event_time"],
                }))
                logger.warning(f"[Job3] Pas de vehicule pour {e['trip_id']}")

    def _find_vehicle(self, zone_id):
        zones = [zone_id] + ADJACENT.get(zone_id, [])
        best_taxi, best_ts = None, float('inf')
        for taxi_id in list(self._vehicles.keys()):
            v = json.loads(self._vehicles.get(taxi_id))
            if v["status"] == "available" and v["zone_id"] in zones:
                if v["last_seen"] < best_ts:
                    best_ts = v["last_seen"]
                    best_taxi = (taxi_id, v["zone_id"])
        return best_taxi

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
            # IF NOT EXISTS = idempotent
            self._stmt = self._session.prepare("""
                INSERT INTO trips
                  (city,date_bucket,created_at,trip_id,rider_id,
                   taxi_id,origin_zone,dest_zone,status,fare,eta_seconds)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                IF NOT EXISTS
            """)
            logger.info("[Job3] Cassandra connecte")

    def map(self, json_str):
        try:
            self._connect()
            e   = json.loads(json_str)
            now = datetime.now(tz=timezone.utc)
            eta = e.get("eta_seconds", 0)
            dist_km = (max(eta,0) / 3600) * AVG_SPEED_KMH
            fare = round(5.0 + dist_km * 3.0, 2)
            self._session.execute(self._stmt,(
                CITY,
                now.strftime("%Y-%m-%d"),
                now,
                uuid.UUID(e["trip_id"]) if len(e["trip_id"])==36 else uuid.uuid4(),
                e["rider_id"],
                e["taxi_id"],
                e["origin_zone"],
                e["dest_zone"],
                e["status"],
                fare,
                eta,
            ))
            logger.info(f"[Job3] Trip ecrit | {e['status']} | ETA={eta}s | Fare={fare}MAD")
        except Exception as ex:
            logger.error(f"[Job3] Erreur Cassandra: {ex}")
        return json_str

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.enable_checkpointing(60_000)
    env.get_checkpoint_config().set_checkpoint_storage_dir("file:///tmp/flink-checkpoints/job3")

    watermark = (WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(3))
        .with_idleness(Duration.of_seconds(10)))

    gps_source = (KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.gps")
        .set_group_id("flink-job3-gps-v1")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())

    trips_source = (KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.trips")
        .set_group_id("flink-job3-trips-v1")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())

    gps_stream = (env.from_source(gps_source, watermark, "GPS-Job3")
        .map(ParseEvent(), output_type=Types.STRING())
        .filter(FilterValid()))

    trips_stream = (env.from_source(trips_source, watermark, "Trips-Job3")
        .map(ParseEvent(), output_type=Types.STRING())
        .filter(FilterValid()))

    (gps_stream.union(trips_stream)
        .key_by(lambda e: json.loads(e)["zone_id"])
        .process(TripMatcher())
        .map(WriteCassandra(), output_type=Types.STRING())
        .print())

    env.execute("TaaSim - Job3 - Trip Matcher")

if __name__ == "__main__":
    main()
