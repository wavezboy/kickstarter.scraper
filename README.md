# Kickstarter Data Pipeline

A robust data pipeline for collecting and analyzing Kickstarter project data.

## Features

- Asynchronous data fetching with aiohttp
- Redis caching with connection pooling
- PostgreSQL database with connection pooling
- Data quality validation and scoring
- Prometheus metrics and monitoring
- Structured logging
- Error handling with retries and backoff
- Data archiving and cleanup
- Advanced analytics with pandas

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables in `.env`:
```
DB_NAME=kickstarter_db
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
REDIS_HOST=localhost
REDIS_PORT=6379
KICKSTARTER_API_URL=your_api_url
```

3. Run the pipeline:
```bash
python robot-io.py
```

## Monitoring

- Prometheus metrics available at :8000/metrics
- Structured logs output in JSON format
- Data quality metrics tracked in real-time

## License

MIT