FROM hseeberger/scala-sbt:8u212_1.2.8_2.13.0 AS builder
COPY ./spark/Analyzer/src/ /Analyzer/src/
COPY ./spark/Analyzer/project.sbt /Analyzer/
RUN cd /Analyzer && sbt package

FROM openjdk:8-jre-slim AS dev
ENV SPARK_VERSION=2.4.3
ENV HADOOP_VERSION=2.7
RUN apt-get update && apt-get -y install \
        wget \
    && apt-get clean
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
ENV PATH="/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/:${PATH}"
RUN mkdir -p /workdir

FROM dev AS runner
COPY --from=builder  /Analyzer/target/scala-2.11/simbad-analyzer_2.11-1.0.jar /jar/analyzer.jar

FROM runner AS stream_converter
CMD spark-submit --master local --class analyzer.StreamLoader /jar/analyzer.jar /data/stream.csv.gz /data/stream.parquet

FROM runner AS analyzer
CMD spark-submit --master local --class analyzer.StreamLoader /jar/analyzer.jar /data/stream.csv.gz /data/output_data