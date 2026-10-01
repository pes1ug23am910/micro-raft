# Run the Linux experiment harness against an existing Docker engine.
FROM docker:29.8.1-cli
RUN apk add --no-cache python3
WORKDIR /work
ENTRYPOINT ["python3", "/work/scripts/cluster_experiment.py"]
