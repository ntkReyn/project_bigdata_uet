-- Bảng cho kết quả xử lý lô (Spark)
CREATE TABLE IF NOT EXISTS data_airline (
    id SERIAL PRIMARY KEY,
    airline TEXT,
    sentiment TEXT,
    negativereason TEXT
);

CREATE TABLE IF NOT EXISTS total_sentiments (
    id SERIAL PRIMARY KEY,
    airline TEXT,
    total_positive INT,
    total_negative INT,
    total_neutral INT
);

CREATE TABLE IF NOT EXISTS total_negativereasons (
    id SERIAL PRIMARY KEY,
    negativereason   TEXT,
    total INT
);


-- Bảng cho kết quả xử lý luồng (Flink)

CREATE TABLE IF NOT EXISTS stream_comments (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMP,
    airline TEXT,
    text TEXT,
    sentiment TEXT    
);

