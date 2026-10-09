# TaaSim — Real-Time Urban Mobility Platform

### Optimizing Urban Mobility with Big Data and Machine Learning

How can urban mobility be optimized when taxis drive empty while passengers wait several minutes for a ride?

The lack of real-time visibility into transportation supply and demand is a major challenge in large cities like Casablanca, Morocco. This imbalance leads to inefficient vehicle utilization, longer passenger waiting times, and a poor user experience.

As part of the **Advanced Big Data** module at ENSA Al Hoceima, we designed and developed **TaaSim**, a demand-responsive urban mobility simulation platform aimed at improving transportation efficiency through real-time data processing and predictive analytics.

The platform combines Big Data technologies and Machine Learning to simulate urban mobility, anticipate passenger demand, and improve the allocation of available taxis.

## Key Features

- **Real-Time Ride Matching:** Automatically matches passenger requests with available taxis.
- **Demand Forecasting:** Predicts future transportation demand across different urban zones.
- **Geographic Remapping:** Adapts trip trajectories to support simulations based on Casablanca.
- **Real-Time Monitoring:** Tracks operational KPIs and system performance through interactive dashboards.
- **Data-Driven Optimization:** Uses large-scale data processing and predictive models to support more efficient mobility operations.

## Technology Stack

| Technology | Purpose |
|---|---|
| Apache Kafka | Real-time data ingestion and event streaming |
| Apache Flink | Real-time stream processing |
| PySpark | Large-scale data analysis and Machine Learning |
| Apache Cassandra | Distributed data persistence |
| FastAPI | REST API development |
| Grafana | Monitoring and interactive dashboards |
| MinIO | Data Lake storage |

## Key Results

- **1.7M+ trips** used as a proxy dataset for the Casablanca mobility context.
- **89,688 trajectories** generated after geographic remapping.
- **63.9% improvement** in prediction model performance compared with the baseline.
- **Less than 5 seconds** of ride-matching latency.

*Results are based on the project's simulation and experimental evaluation.*

## Project Workflow

1. **Data Ingestion:** Collects and streams mobility data using Apache Kafka.
2. **Stream Processing:** Processes real-time events with Apache Flink.
3. **Data Analysis:** Applies PySpark for large-scale analysis and demand prediction.
4. **Data Storage:** Uses MinIO for data lake storage and Cassandra for distributed persistence.
5. **Ride Matching:** Matches passenger requests with available taxis.
6. **Monitoring:** Visualizes operational indicators and system performance using Grafana.

## Project Objectives

- Improve the coordination between taxi supply and passenger demand.
- Reduce vehicle idle time and passenger waiting time.
- Forecast mobility demand across geographic zones.
- Explore real-time Big Data architectures for urban transportation.
- Evaluate how Machine Learning can support data-driven mobility optimization.

## Skills Developed

**Data Engineering · Big Data · Real-Time Stream Processing · Apache Kafka · Apache Flink · PySpark · Machine Learning · Distributed Databases · REST APIs · Data Visualization**

---

*Developed as part of the Advanced Big Data module at ENSA Al Hoceima.*
