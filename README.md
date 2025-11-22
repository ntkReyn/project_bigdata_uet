# project_bigdata

Cấu trúc: (mô tả ngắn)
- data/: chứa `airline_sentiment.csv`
- flink/: job PyFlink, Dockerfile, requirements
- spark/: job PySpark, Dockerfile, requirements
- simulator/: script giả lập gửi dữ liệu lên Kafka
- web_app/: Flask app + dashboard
- postgres/: init.sql để tạo bảng khi Postgres khởi động
- docker-compose.yml để khởi tạo stack (Postgres, Zookeeper, Kafka, simulator, flink, spark, web_app)

