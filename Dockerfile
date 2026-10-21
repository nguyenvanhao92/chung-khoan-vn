FROM python:3.12-slim
WORKDIR /app
COPY server.py VN-Stock-Analyzer.html ./
ENV HOST=0.0.0.0 PORT=8000 NO_BROWSER=1 PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["python", "server.py"]
