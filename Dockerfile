# syntax=docker/dockerfile:1
# One Dockerfile for both services: build with --build-arg MODULE=api or MODULE=worker.

FROM maven:3.9-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl ${MODULE} -am package -DskipTests

FROM eclipse-temurin:21-jre
ARG MODULE
WORKDIR /app
COPY --from=build /src/${MODULE}/target/${MODULE}.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
