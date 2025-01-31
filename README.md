# README - Projet d'Analyse en Temps Réel

## Description
Ce projet met en place un pipeline de traitement en temps réel utilisant les technologies suivantes :
- **Python** pour la consommation des données et l'envoi vers Kafka
- **Apache Kafka** pour le streaming des données
- **Apache Spark (PySpark Streaming)** pour le traitement et l'analyse des données
- **Apache Cassandra** pour le stockage des résultats d'analyse
- **Grafana** pour la visualisation des analyses en temps réel

---

## Structure du Projet

```
FINTRANS-BIG-DATA-ANALYTICS-FOR-PUBLIC-T...
│── Api/
│   ├── données_statique.ipynb
│   ├── gtfs_realtime_to_kafka.ipynb
│   ├── MQTT.ipynb
│   ├── requirements.txt
│── cassandra/
│   ├── cassandra.sql
│── kafka/
│── SparkStream/
│   ├── checkpoint/
│   ├── chetest/
│   ├── dossier_parquet/
│   ├── JsonNode/
│── spark-warehouse/
│   ├── Analyses.ipynb
│   ├── graphe.ipynb
│   ├── KafkaConsumer.ipynb
│   ├── SparkApp.ipynb
│   ├── transformation.ipynb
│── README.md
│── requirements.txt
```

---

## Prérequis
Avant d'exécuter ce projet, assurez-vous d'avoir installé les outils suivants :

1. **Docker et Docker Compose** (facultatif, pour un déploiement simplifié)
2. **Python 3.x** avec les bibliothèques suivantes :
   ```bash
   pip install kafka-python pyspark cassandra-driver pandas
   ```
3. **Apache Kafka**
4. **Apache Spark** (avec PySpark installé)
5. **Apache Cassandra**
6. **Grafana**

---

## Étapes d'Exécution

### 1. Démarrer les services (Kafka, Cassandra, Grafana)

Si vous utilisez Docker, vous pouvez créer un fichier `docker-compose.yml` pour démarrer ces services facilement.
Sinon, démarrez les services manuellement :

#### Démarrer Kafka
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &
```
Créer un topic Kafka pour la diffusion des données :
```bash
bin/kafka-topics.sh --create --topic real_time_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

#### Démarrer Cassandra
```bash
sudo systemctl start cassandra
```
Créer une base de données et une table :
```cql
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE test;
```
Créer les tables en utilisant le ficher **cassandra.sql**

#### Démarrer Grafana
```bash
sudo systemctl start grafana-server
```
Accédez à Grafana via `http://localhost:3000` et configurez la connexion à Cassandra.

---

### 2. Exécuter le script de production des données (Python → Kafka)
Le script  Python  **MQTT.ipynb** envoie des données  vers Kafka :


**Visualisation avec Grafana**

1/ Accédez à Grafana à l'adresse http://localhost:3000.
2/ Configurez Cassandra comme source de données.



