# Use an appropriate base image that includes Java and wget
FROM openjdk:11-jre-slim

# Set environment variables
ENV FUSEKI_VERSION 4.9.0
ENV FUSEKI_HOME /fuseki

# Install wget
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Download and extract Fuseki
RUN wget -qO- https://archive.apache.org/dist/jena/binaries/apache-jena-fuseki-$FUSEKI_VERSION.tar.gz | tar xvz -C / && \
    mv /apache-jena-fuseki-$FUSEKI_VERSION $FUSEKI_HOME

# Expose the default port
EXPOSE 3030

# Download and extract Jena Commands
RUN wget -qO- https://archive.apache.org/dist/jena/binaries/apache-jena-$FUSEKI_VERSION.tar.gz | tar xvz -C / && \
    mv /apache-jena-$FUSEKI_VERSION $FUSEKI_HOME

# Copy the Fuseki configuration file to the container
COPY ./nmdc_runtime/site/fuseki/fuseki-config.ttl $FUSEKI_HOME/configuration/
COPY ./nmdc_runtime/site/fuseki/shiro.ini $FUSEKI_HOME/run/

# Set working directory
WORKDIR $FUSEKI_HOME

# Command to start Fuseki server with preloaded data
CMD ["./fuseki-server", "--config", "configuration/fuseki-config.ttl"]