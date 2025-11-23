# project_bigdata

Cấu trúc: (mô tả ngắn)
- data/: chứa `airline_sentiment.csv`
- flink/: job PyFlink, Dockerfile, requirements
- spark/: job PySpark, Dockerfile, requirements
- reddit_crawler/: script lấy dữ liệu real-time từ bình luận trên các nhóm trên reddit
- web_app/: Flask app + dashboard
- postgres/: init.sql để tạo bảng khi Postgres khởi động
- docker-compose.yml để khởi tạo stack (Postgres, Zookeeper, Kafka, reddit_crawler, flink, spark, web_app)

