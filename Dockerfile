FROM hseeberger/scala-sbt:8u212_1.2.8_2.13.0 AS build
COPY ./spark/Analyzer/src ./spark/Analyzer/project.sbt /Analyzer/
RUN cd /Analyzer && sbt package