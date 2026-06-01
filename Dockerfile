FROM apache/spark:4.0.0

USER root

COPY requirements.txt /tmp/

RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER spark