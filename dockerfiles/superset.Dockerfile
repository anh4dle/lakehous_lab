FROM apache/superset:latest

USER root

# Bootstrap pip inside Superset's virtual environment and install database connectors
RUN /app/.venv/bin/python -m ensurepip && \
    /app/.venv/bin/python -m pip install --no-cache-dir trino sqlalchemy-trino psycopg2-binary

USER superset