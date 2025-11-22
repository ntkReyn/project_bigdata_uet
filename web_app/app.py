from flask import Flask, render_template, jsonify, request
import psycopg2
import os

app = Flask(__name__)

# Lấy thông tin kết nối DB
def get_db_connection():
    conn = psycopg2.connect(
        host="postgres",
        database="bigdata_db",
        user="admin",
        password="admin"
    )
    return conn

# === Trang Chính (Render Dashboard) ===
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/batch_totals')
def api_batch_totals():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                SUM(total_positive),
                SUM(total_negative),
                SUM(total_neutral)
            FROM total_sentiments
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()

        data = {
            "labels": ["Positive", "Negative", "Neutral"],
            "datasets": [
                {
                    "label": "Tổng Sentiment",
                    "data": [row[0], row[1], row[2]],
                    "backgroundColor": ['rgba(75, 192, 192, 0.6)', 
                                        'rgba(255, 99, 132, 0.6)', 
                                        'rgba(201, 203, 207, 0.6)'],
                    "hoverOffset": 4
                }
            ]
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/api/batch_grouped')
def api_batch_grouped():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT airline, total_positive, total_negative, total_neutral
            FROM total_sentiments
            ORDER BY (total_positive + total_negative + total_neutral) DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        labels = [r[0] for r in rows]
        positive_data = [r[1] for r in rows]
        negative_data = [r[2] for r in rows]
        neutral_data = [r[3] for r in rows]

        data = {
            "labels": labels,
            "datasets": [
                {"label": "Positive", "data": positive_data, "backgroundColor": 'rgba(75, 192, 192, 0.6)'},
                {"label": "Negative", "data": negative_data, "backgroundColor": 'rgba(255, 99, 132, 0.6)'},
                {"label": "Neutral", "data": neutral_data, "backgroundColor": 'rgba(201, 203, 207, 0.6)'}
            ]
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/api/batch_negativereason_total')
def api_batch_negativereason_total():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT negativereason, total
            FROM total_negativereasons
            ORDER BY total DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        labels = [r[0] if r[0] else "Unknown" for r in rows]
        data_counts = [r[1] for r in rows]

        data = {
            "labels": labels,
            "datasets": [
                {
                    "label": "Tổng Negative Reasons",
                    "data": data_counts,
                    "backgroundColor": "rgba(255, 99, 132, 0.6)"
                }
            ]
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/api/batch_negativereason_by_airline')
def api_batch_negativereason_by_airline():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Lấy tổng từng lý do negative theo từng hãng
        cur.execute("""
            SELECT airline, negativereason, COUNT(*) as total_count
            FROM data_airline
            WHERE sentiment = 'negative'
            GROUP BY airline, negativereason
            ORDER BY airline, total_count DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Chuyển dữ liệu thành dict {airline: {labels: [], data: []}}
        result = {}
        for row in rows:
            airline = row[0] if row[0] else "Unknown"
            reason = row[1] if row[1] else "Unknown"
            count = row[2]
            if airline not in result:
                result[airline] = {"labels": [], "data": []}
            result[airline]["labels"].append(reason)
            result[airline]["data"].append(count)

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/airline_sentiment_avg')
def api_airline_sentiment_avg():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Tính điểm trung bình cảm xúc theo hãng
        cur.execute("""
            SELECT 
                airline,
                (total_positive * 1.0 + total_neutral * 0.5 + total_negative * 0.0) 
                / NULLIF((total_positive + total_neutral + total_negative), 0) AS avg_score
            FROM total_sentiments
            ORDER BY airline;
        """)

        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Chuyển dữ liệu sang format Chart.js
        labels = [row[0] for row in rows]
        values = [float(row[1]) for row in rows]

        data = {
            "labels": labels,
            "datasets": [
                {
                    "label": "Điểm cảm xúc trung bình",
                    "data": values,
                    "backgroundColor": "rgba(54, 162, 235, 0.6)"
                }
            ]
        }

        return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/api/stream_trending_totals_realtime')
def api_stream_trending_totals_realtime():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Tổng số comment theo sentiment mỗi 5s
        cur.execute("""
            WITH bucketed AS (
                SELECT 
                    to_timestamp(floor(extract('epoch' from event_time) / 5) * 5) AS time_bucket,
                    sentiment,
                    COUNT(*) AS total_count
                FROM stream_comments
                GROUP BY time_bucket, sentiment
            ),
            latest_20 AS (
                SELECT DISTINCT time_bucket
                FROM bucketed
                ORDER BY time_bucket DESC
                LIMIT 20
            )
            SELECT b.time_bucket, b.sentiment, b.total_count
            FROM bucketed b
            JOIN latest_20 l ON b.time_bucket = l.time_bucket
            ORDER BY b.time_bucket ASC;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Chuyển dữ liệu thành Chart.js
        time_labels = sorted(list({r[0].strftime('%H:%M:%S') for r in rows}))
        sentiments = sorted(list({r[1].capitalize() for r in rows}))

        data_map = {s: [0] * len(time_labels) for s in sentiments}
        for r in rows:
            t_idx = time_labels.index(r[0].strftime('%H:%M:%S'))
            s = r[1].capitalize()
            data_map[s][t_idx] = r[2]

        datasets = [
            {
                "label": sentiment,
                "data": data_map[sentiment],
                "backgroundColor": color
            } for sentiment, color in zip(sentiments, ['rgba(75, 192, 192, 0.6)',
                                                        'rgba(255, 99, 132, 0.6)',
                                                        'rgba(201, 203, 207, 0.6)'])
        ]

        return jsonify({"labels": time_labels, "datasets": datasets})

    except Exception as e:
        return jsonify({"error": str(e)})



# -----------------------------
# Tổng sentiment theo hãng theo thời gian
# -----------------------------
@app.route('/api/stream_trending_by_airline_realtime')
def api_stream_trending_by_airline_realtime():
    try:
        airline = request.args.get("airline")
        if not airline:
            return jsonify({"error": "Missing airline parameter"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()

        # Tổng comment theo sentiment mỗi 5s cho hãng
        cur.execute("""
            SELECT 
                to_timestamp(floor(extract('epoch' from event_time) / 5) * 5) AS time_bucket,
                sentiment,
                COUNT(*) AS total_count
            FROM stream_comments
            WHERE airline = %s
            GROUP BY time_bucket, sentiment
            ORDER BY time_bucket ASC;
        """, (airline,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Chuyển dữ liệu thành format Chart.js
        time_labels = sorted(list({r[0].strftime('%H:%M:%S') for r in rows}))
        sentiments = sorted(list({r[1].capitalize() for r in rows}))
        data_map = {s: [0] * len(time_labels) for s in sentiments}

        for r in rows:
            t_idx = time_labels.index(r[0].strftime('%H:%M:%S'))
            s = r[1].capitalize()
            data_map[s][t_idx] = r[2]

        datasets = [
            {
                "label": sentiment,
                "data": data_map[sentiment],
                "backgroundColor": color
            } for sentiment, color in zip(sentiments, ['rgba(75, 192, 192, 0.6)',
                                                        'rgba(255, 99, 132, 0.6)',
                                                        'rgba(201, 203, 207, 0.6)'])
        ]

        return jsonify({"labels": time_labels, "datasets": datasets})

    except Exception as e:
        return jsonify({"error": str(e)})


# -----------------------------
# Latest comments theo thời gian
# -----------------------------
@app.route('/api/stream_latest_comments_realtime')
def api_stream_latest_comments_realtime():
    try:
        airline = request.args.get("airline")  # tùy chọn: lọc theo hãng
        limit = int(request.args.get("limit", 10))  # số comment muốn lấy, default=10

        conn = get_db_connection()
        cur = conn.cursor()

        if airline:
            # Lấy bình luận gần nhất của hãng cụ thể
            cur.execute("""
                SELECT airline, sentiment, text, TO_CHAR(event_time, 'HH24:MI:SS') as time_label
                FROM stream_comments
                WHERE airline = %s
                ORDER BY event_time DESC
                LIMIT %s
            """, (airline, limit))
        else:
            # Lấy bình luận gần nhất của tất cả hãng
            cur.execute("""
                SELECT airline, sentiment, text, TO_CHAR(event_time, 'HH24:MI:SS') as time_label
                FROM stream_comments
                ORDER BY event_time DESC
                LIMIT %s
            """, (limit,))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Chuyển dữ liệu sang dạng JSON
        comments = [
            {"airline": r[0], "sentiment": r[1], "text": r[2], "time": r[3]}
            for r in rows
        ]

        return jsonify(comments)

    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/api/stream_sentiment_score_realtime')
def api_stream_sentiment_score_realtime():
    try:
        interval_sec = int(request.args.get("interval_sec", 5))

        conn = get_db_connection()
        cur = conn.cursor()

        # Tính điểm cảm xúc trung bình theo bucket
        cur.execute(f"""
            WITH bucketed AS (
                SELECT 
                    airline,
                    to_timestamp(floor(extract('epoch' from event_time) / {interval_sec}) * {interval_sec}) AS time_bucket,
                    AVG(
                        CASE sentiment
                            WHEN 'positive' THEN 1
                            WHEN 'neutral' THEN 0.5
                            WHEN 'negative' THEN 0
                        END
                    ) AS avg_score
                FROM stream_comments
                GROUP BY airline, time_bucket
            ),
            latest_20 AS (
                SELECT DISTINCT time_bucket
                FROM bucketed
                ORDER BY time_bucket DESC
                LIMIT 20
            )
            SELECT b.airline, b.time_bucket, b.avg_score
            FROM bucketed b
            JOIN latest_20 l ON b.time_bucket = l.time_bucket
            ORDER BY b.time_bucket ASC, b.airline;

        """, (interval_sec,))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Tạo time_labels và data_map
        time_labels = sorted(list({r[1].strftime('%H:%M:%S') for r in rows}))
        data_map = {}

        for r in rows:
            airline = r[0] if r[0] else "Unknown"
            t_idx = time_labels.index(r[1].strftime('%H:%M:%S'))
            score = float(r[2])
            if airline not in data_map:
                data_map[airline] = [0] * len(time_labels)
            data_map[airline][t_idx] = score

        # Chuyển sang format Chart.js
        datasets = [
            {
                "label": airline,
                "data": scores,
                "backgroundColor": 'rgba(54, 162, 235, 0.6)'
            } for airline, scores in data_map.items()
        ]

        return jsonify({"labels": time_labels, "datasets": datasets})

    except Exception as e:
        return jsonify({"error": str(e)})




if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)