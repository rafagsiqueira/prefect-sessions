# Use the official Prefect base image matching your desired version
FROM prefecthq/prefect:3-python3.12-kubernetes

# Set a working directory
WORKDIR /app

RUN pip install --no-cache-dir prefect-aca-sessions==2.2.0

COPY container-entrypoint.sh /usr/local/bin/container-entrypoint.sh
RUN chmod +x /usr/local/bin/container-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/container-entrypoint.sh"]

EXPOSE 8080

# Run the prefect worker command as the main execution process
CMD ["python", "-m", "prefect_aca_sessions.agent"]
