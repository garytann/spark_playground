# Use a base image with a Java Runtime Environment
FROM spark:4.0.1-scala2.13-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y r-base r-base-dev; \
    rm -rf /var/lib/apt/lists/*

ENV R_HOME=/usr/lib/R

USER spark

WORKDIR /app

COPY spark_playground/target/spark_playground-1.0-SNAPSHOT.jar /app/spark_playground-1.0-SNAPSHOT.jar

ENTRYPOINT ["spark-submit", \
                "--class", "com.example.Main", \
                "--master", "local[*]", \
                "/app/spark_playground-1.0-SNAPSHOT.jar", \
		10,\
		"inputFilePath1",\
		"inputFilePath2",\
		"outputFilePath]


