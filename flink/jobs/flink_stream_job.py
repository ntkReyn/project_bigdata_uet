from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.expressions import col

def run_flink_stream_job():
    print("--- Bắt đầu Flink Stream Job (Simple Pass-through) ---")

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)

    # 1. NGUỒN: Kafka (Đã có cột 'nhan' do Crawler gửi lên)
    t_env.execute_sql("""
        CREATE TABLE reddit_source (
            `id` BIGINT,
            `event_time` STRING,
            `airline` STRING,
            `text` STRING,
            `sentiment` STRING  -- Nhận nhãn trực tiếp từ Kafka
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'reddit_topic',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink_simple_consumer',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # 2. ĐÍCH: Postgres
    t_env.execute_sql("""
        CREATE TABLE stream_results_sink (
            `id` BIGINT,
            `event_time` TIMESTAMP(3),
            `airline` STRING,
            `text` STRING,
            `sentiment` STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/bigdata_db?sslmode=disable',
            'table-name' = 'stream_comments',
            'username' = 'admin',
            'password' = 'admin',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # 3. Chuyển đổi và Ghi (Chỉ cần cast thời gian)
    source_table = t_env.from_path("reddit_source")
    
    result_table = source_table.select(
        col("id"),
        col("event_time").cast(DataTypes.TIMESTAMP(3)), # Chuyển String -> Timestamp
        col("airline"),
        col("text"),
        col("sentiment") # Giữ nguyên nhãn
    )

    print(">>> Submit Job...")
    result_table.execute_insert("stream_results_sink").wait()

if __name__ == "__main__":
    run_flink_stream_job()