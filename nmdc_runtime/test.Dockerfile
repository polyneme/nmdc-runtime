# Best practice: Choose a stable base image and tag.
FROM python:3.10-slim-bullseye

# Install security updates, and some useful packages.
#
# Best practices:
# * Make sure apt-get doesn't run in interactive mode.
# * Update system packages.
# * Pre-install some useful tools.
# * Minimize system package installation.
RUN export DEBIAN_FRONTEND=noninteractive && \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get install -y --no-install-recommends tini procps net-tools \
  build-essential git make zip wget gnupg && \
  apt-get -y clean && \
  rm -rf /var/lib/apt/lists/*

# Install requirements, incl. for tests
WORKDIR /code
COPY ./requirements/dev.txt /code/requirements.dev.txt
RUN pip install --no-cache-dir -r /code/requirements.dev.txt
COPY ./requirements/main.txt /code/requirements.txt
RUN pip install --no-cache-dir -r /code/requirements.txt


# Add repository code
COPY . /code
RUN pip install --no-cache-dir --editable .

## Ensure wait-for-it
RUN chmod +x wait-for-it.sh

# Best practices: Prepare for C crashes.
ENV PYTHONFAULTHANDLER=1


# uncomment line below to run all tests
# ENTRYPOINT [ "./wait-for-it.sh", "fastapi:8000" , "--strict" , "--timeout=300" , "--" , "pytest"]

# uncomment line below to stop after first test failure:
# https://docs.pytest.org/en/6.2.x/usage.html#stopping-after-the-first-or-n-failures
ENTRYPOINT [ "./wait-for-it.sh", "fastapi:8000" , "--strict" , "--timeout=300" , "--" , "pytest", "--cov=nmdc_runtime"]
