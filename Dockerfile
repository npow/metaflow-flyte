FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    "metaflow>=2.9" \
    "flytekit>=1.10" \
    "boto3>=1.26"

# Copy the metaflow-flyte package and install it
COPY metaflow_extensions/ /pkg/metaflow_extensions/
COPY pyproject.toml /pkg/
RUN pip install --no-cache-dir -e /pkg/

# Copy the test flows to the same absolute path as on the host machine
# This ensures FLOW_FILE in generated files matches the container path
RUN mkdir -p /Users/npow/code/metaflow-flyte/tests/flows
COPY tests/flows/ /Users/npow/code/metaflow-flyte/tests/flows/

# MinIO / S3 credentials for the Flyte sandbox
# The sandbox MinIO is accessible at this in-cluster address
ENV USERNAME=metaflow \
    AWS_ACCESS_KEY_ID=minio \
    AWS_SECRET_ACCESS_KEY=miniostorage \
    AWS_DEFAULT_REGION=us-east-1 \
    METAFLOW_S3_ENDPOINT_URL=http://flyte-sandbox-minio.flyte.svc.cluster.local:9000 \
    METAFLOW_DATASTORE_SYSROOT_S3=s3://metaflow-flyte/

WORKDIR /workspace
