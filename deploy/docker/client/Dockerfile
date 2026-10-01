FROM python:3.13-slim-trixie
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN groupadd --gid 10001 client \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin client \
    && mkdir -p /ledger \
    && chown 10001:10001 /ledger
COPY client.py /app/client.py
USER 10001:10001
VOLUME ["/ledger"]
ENTRYPOINT ["python", "/app/client.py"]
