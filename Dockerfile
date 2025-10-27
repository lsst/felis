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

# Copy the entire application
COPY . /app

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir setuptools wheel build lsst-versions && \
    pip install --no-cache-dir --no-build-isolation . psycopg2-binary

FROM base-image AS runtime-image
# Create a non-root user with UID 1001
RUN useradd --create-home --uid 1001 felis

# Copy the entire app
COPY --from=install-image /app /app

# Set the working directory
WORKDIR /app

# Switch to the non-root user
USER felis

# Python packages are already in system site-packages
ENV PATH="/usr/local/bin:$PATH"

# Run bash by default
CMD ["bash"]
