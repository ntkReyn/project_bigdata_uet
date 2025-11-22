# Sử dụng image Python 3.9
FROM python:3.9-slim

WORKDIR /app

# Cài đặt các thư viện Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code của web_app vào
COPY . .

# Chạy ứng dụng bằng Gunicorn (production web server)
# Nó sẽ chạy file 'app.py' (tên file là 'app'),
# và biến 'app' (là 'app = Flask(__name__)')
CMD ["gunicorn", "--workers=4", "--bind=0.0.0.0:5000", "app:app"]