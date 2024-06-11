FROM liliasfaxi/spark-hadoop:hv-2.7.2

# Installer curl et bash
USER root
RUN apt-get update && apt-get install -y curl bash
RUN apt-get update && apt-get install -y dos2unix

# Copy the configuration files
COPY hadoop-env.sh /usr/local/hadoop/etc/hadoop/hadoop-env.sh
COPY core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml
COPY hdfs-site.xml /usr/local/hadoop/etc/hadoop/hdfs-site.xml

# Copy the entrypoint script
COPY entrypoint.sh /entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /entrypoint.sh

# Use the entrypoint script as the container's entrypoint
#ENTRYPOINT ["/entrypoint.sh"]