FROM python:3.10-slim

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY . .

# Команда запуска: 
# 1. Пытается скопировать service_account.json из секретов Render
# 2. Запускает бота
CMD ["sh", "-c", "cp /etc/secrets/service_account.json . 2>/dev/null || : && python bot.py"]
