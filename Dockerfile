# Dockerfile for running the felis-cli
FROM python:3.13.7-slim-trixie AS base-image
# Update system packages
COPY scripts/install-base-packages.sh .
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    ./install-base-packages.sh && rm ./install-base-packages.sh

FROM base-image AS install-image
# Install system packages only needed for building dependencies
COPY scripts/install-dependency-packages.sh .
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    ./install-dependency-packages.sh

# Install the dependencies and application
WORKDIR /app

# Copy the entire application (including .git for version detection)
COPY . /app

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m venv /app/.venv && \
    /app/.venv/bin/pip install --no-cache-dir . psycopg2-binary

FROM base-image AS runtime-image
# Create a non-root user with UID 1001
RUN useradd --create-home --uid 1001 felis
# Copy the virtualenv
COPY --from=install-image /app/.venv /app/.venv
# Set the working directory
WORKDIR /app
# Switch to the non-root user
USER felis
# Make sure we use the virtualenv
ENV PATH="/app/.venv/bin:$PATH"
# Run bash by default
CMD ["bash"]
