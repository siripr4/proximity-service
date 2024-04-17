CREATE TABLE geospatial_index (
    geohash TEXT PRIMARY KEY,
    business_id INTEGER REFERENCES businesses(id)
)