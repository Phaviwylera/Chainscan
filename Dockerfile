FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 CHAINSCAN_FLUSH_MB=128
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY chainscan ./chainscan
COPY contracts ./contracts
COPY config.example.yaml config.render.yaml README.md ./
EXPOSE 10000
ENTRYPOINT ["python", "-m", "chainscan"]
CMD ["web"]
