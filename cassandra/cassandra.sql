CREATE KEYSPACE test 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};



CREATE TABLE IF NOT EXISTS vehicle_data_st (
    trip_id TEXT,
    tst TEXT,
    vehicle_type TEXT,
    nextStop TEXT,
    desi TEXT,
    dl INT,
    spd DOUBLE,
    lat DOUBLE,
    long DOUBLE,
    route_id TEXT,
    start_date TEXT,
    start_time TEXT,
    arrival_time TIMESTAMP,
    departure_time TIMESTAMP,
    PRIMARY KEY ((trip_id), tst)
);

CREATE TABLE fleet_performance (
    vehicle_type text,
    window_start timestamp,
    window_end timestamp,
    avg_speed float,
    active_vehicles int,
    max_speed float,
    min_speed float,
    avg_occupancy float,
    PRIMARY KEY (vehicle_type, window_start)
);

CREATE TABLE IF NOT EXISTS analyses_temps_reel_line (
    line INT PRIMARY KEY,
    vitesse_moyenne DOUBLE,
    vitesse_min DOUBLE,
    vitesse_max DOUBLE,
    retard_moyen DOUBLE,
    nb_vehicules_actifs BIGINT
);



#utill
CREATE TABLE IF NOT EXISTS traffic_analysis (
    line INT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    vitesse_moyenne DOUBLE,
    vitesse_min DOUBLE,
    vitesse_max DOUBLE,
    retard_moyen DOUBLE,
    nb_vehicules_actifs BIGINT,
    PRIMARY KEY ((line), window_start)
) WITH CLUSTERING ORDER BY (window_start ASC);
