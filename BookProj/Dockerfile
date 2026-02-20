FROM python:3.12.4-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# نصب وابستگی‌های سیستم
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app

# کپی و نصب وابستگی‌های پایتون
COPY ./requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# کپی کل پروژه
COPY . /app/

CMD ["sh", "-c", "python manage.py migrate && python manage.py runserver 0.0.0.0:8001"]