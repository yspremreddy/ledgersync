FROM python:3.13.2-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HOME=/tmp

WORKDIR /srv/ledgersync
COPY ui/requirements.txt /tmp/requirements.txt
RUN pip install --disable-pip-version-check --requirement /tmp/requirements.txt \
    && rm /tmp/requirements.txt
COPY ui /srv/ledgersync/ui
COPY tests/test_ui_client.py /srv/ledgersync/tests/test_ui_client.py

USER 10001:10001
EXPOSE 8501
CMD ["streamlit", "run", "ui/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--browser.gatherUsageStats=false"]
