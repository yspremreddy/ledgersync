FROM python:3.13.2-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv/ledgersync
COPY app/requirements.txt /tmp/requirements.txt
RUN pip install --disable-pip-version-check --requirement /tmp/requirements.txt \
    && rm /tmp/requirements.txt
COPY app /srv/ledgersync/app
COPY tests/test_api.py /srv/ledgersync/tests/test_api.py

USER 10001:10001
EXPOSE 18000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "18000", "--no-access-log"]
