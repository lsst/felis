# Dockerfile for running the felis-cli
FROM python:3.13.7-slim-trixie AS base-image

# Update base packages
COPY scripts/install-base-packages.sh .
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    ./install-base-packages.sh && rm ./install-base-packages.sh

FROM base-image AS install-image

# Install dependencies
COPY scripts/install-dependency-packages.sh .
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    ./install-dependency-packages.sh

# Install the application
WORKDIR /app
COPY . /app

# Create venv
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install build tools
RUN --mount=type=cache,target=/root/.cache/pip \
    /opt/venv/bin/pip install --upgrade pip setuptools wheel

# Install application
RUN --mount=type=cache,target=/root/.cache/pip \
    /opt/venv/bin/pip install --no-cache-dir . psycopg2-binary

FROM base-image AS runtime-image

RUN useradd --create-home --uid 1001 felis

# Copy installed app
COPY --from=install-image /opt/venv /opt/venv
COPY --from=install-image /app /app

# Set the working directory
WORKDIR /app

# Set PATH to use venv
ENV PATH="/opt/venv/bin:$PATH"

# Run bash by default
CMD ["bash"]
