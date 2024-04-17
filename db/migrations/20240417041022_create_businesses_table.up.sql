CREATE TABLE businesses (
    id INTEGER PRIMARY KEY,
    name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
)
