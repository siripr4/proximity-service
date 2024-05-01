CREATE TABLE geospatial_index (
    geohash TEXT,
    business_id INTEGER REFERENCES businesses(id),
    PRIMARY KEY (geohash, business_id)
)
