FROM bitnami/spark:latest

USER root

# Add spark user with proper entries
RUN id -u spark &>/dev/null || useradd -u 1001 -g root -m -d /opt/bitnami/spark -s /bin/bash spark
RUN echo 'spark:x:1001:1001:Spark User:/opt/bitnami/spark:/bin/bash' >> /etc/passwd
RUN echo 'spark:x:1001:' >> /etc/group

# Create directory for Ivy cache
RUN mkdir -p /opt/bitnami/spark/.ivy2 && chown -R 1001:root /opt/bitnami/spark/.ivy2

# Switch back to the original bitnami user
USER 1001