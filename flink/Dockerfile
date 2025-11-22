# Dùng image Flink Java gốc (Ổn định nhất)
FROM apache/flink:1.17.1-scala_2.12-java11

USER root

# Cài Python thủ công (Cách này đã chứng minh là chạy được)
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

# Cài thư viện Flink (Rất nhanh vì không có ML)
RUN python -m pip install --upgrade pip
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Tải Drivers (Quan trọng: Dùng link đúng mà bạn đã tìm ra)
RUN curl -L -o /opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar
RUN curl -L -o /opt/flink/lib/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
RUN curl -L -o /opt/flink/lib/flink-connector-jdbc-3.1.2-1.17.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar

USER flink