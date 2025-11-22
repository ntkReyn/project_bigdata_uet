# ... (Phần đầu giữ nguyên)
FROM apache/spark:3.5.1

USER root

# Cài đặt curl
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# --- CÀI ĐẶT DEPENDENCIES CHO S3 (MinIO) ---
# Spark 3.5.1 dùng Hadoop 3.3.4. Chúng ta cần các jar tương thích.
WORKDIR /opt/spark/jars

# 1. Hadoop AWS
RUN curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# 2. AWS Java SDK Bundle (Thư viện giao tiếp S3)
RUN curl -L -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 3. Postgres Driver (Như cũ)
RUN curl -L -o postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Trở lại thư mục mặc định
WORKDIR /opt/spark/work-dir
USER 185