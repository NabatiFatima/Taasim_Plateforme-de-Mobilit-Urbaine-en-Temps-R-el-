import json
import logging

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import CheckpointingMode
from pyflink.common import Configuration
import json
from shapely.geometry import shape, Point

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gps-flink-job")


# =========================================================
# REQUIRED FIELDS
# =========================================================
REQUIRED_FIELDS = [
    "trip_id",
    "taxi_id",
    "timestamp",
    "point_index",
    "lat",
    "lon",
    "speed",
    "status"
]


# =========================================================
# ZONES (CASABLANCA)
# =========================================================
# 1. Load your GeoJSON data
# Inside the container, this is the path you mapped or copied to
GEOJSON_PATH = "/opt/flink/data/raw/Arrondissements.geojson"

with open(GEOJSON_PATH, 'r') as f:
    data = json.load(f)

# 2. Pre-process the zones into Shapely objects for speed
ZONES = []
for feature in data['features']:
    ZONES.append({
        "name": feature['properties'].get('Arrondissement', 'unknown'),
        "polygon": shape(feature['geometry'])
    })


# =========================================================
# ENV
# =========================================================
config = Configuration()

# 1. Hadoop S3A Keys (Correctly updated by you!)
config.set_string("fs.s3a.endpoint", "http://minio:9000")
config.set_string("fs.s3a.access.key", "admin")
config.set_string("fs.s3a.secret.key", "admin123")
config.set_string("fs.s3a.path.style.access", "true")
config.set_string("fs.s3a.connection.ssl.enabled", "false")
config.set_boolean("state.backend.incremental", True)
# 2. Initialize Environment
env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_parallelism(1)

# 3. ADD THESE BACK: Active Checkpointing & Storage Path
env.enable_checkpointing(60000, CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_checkpoint_storage_dir(
    "s3://flink-checkpoints/job1-gps-normalizer"
)
# =========================================================
# KAFKA SOURCE (raw.gps)
# =========================================================
kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("raw.gps") \
    .set_group_id("flink-gps") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()





# =========================================================
# PARSE + VALIDATE
# =========================================================
def parse_event(raw):
    try:
        # 🔥 FORCE STRING
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")

        event = json.loads(raw)

        missing = [f for f in REQUIRED_FIELDS if f not in event]
        if missing:
            logger.warning(f"Missing fields: {missing}")
            return None

        return event

    except Exception as e:
        logger.warning(f"Bad JSON: {raw} | {e}")
        return None


# =========================================================
# VALID COORDINATES
# =========================================================
def valid_coords(event):
    try:
        lat = float(event["lat"])
        lon = float(event["lon"])
        return -90 <= lat <= 90 and -180 <= lon <= 180
    except:
        return False


# =========================================================
# ZONE MAPPING
# =========================================================

def map_zone(event):
    # Create a Point object (Note: GeoJSON is [lon, lat])
    point = Point(event["lon"], event["lat"])

    # Check which polygon contains the point
    for zone in ZONES:
        if zone["polygon"].contains(point):
            event["zone"] = zone["name"]
            return event

    event["zone"] = "unknown"
    return event




# =========================================================
# ENRICHMENT
# =========================================================
# def enrich(event):
#     event["city"] = "casablanca"
#     return event


# =========================================================
# WATERMARKS (event-time handling)
# =========================================================
watermark_strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_minutes(3)) \
    .with_timestamp_assigner(
        lambda event, ts: event["timestamp"] * 1000
    )


# =========================================================
# STREAM SOURCE
# =========================================================


stream = env.from_source(
    kafka_source,
    watermark_strategy,
    "Kafka Source raw.gps"
)


# =========================================================
# PIPELINE
# =========================================================
parsed = stream.map(parse_event).filter(lambda x: x is not None)

filtered = parsed.filter(valid_coords)

mapped = filtered.map(map_zone)


# enriched = mapped.map(enrich)

final_stream = mapped.map(lambda x: json.dumps(x),output_type=Types.STRING())

# =========================================================
# KAFKA SINK (processed.gps)
# =========================================================

kafka_sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic("processed.gps")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ) \
    .build()



final_stream.sink_to(kafka_sink)



# =========================================================
# EXECUTE JOB
# =========================================================
env.execute("GPS Flink Pipeline")