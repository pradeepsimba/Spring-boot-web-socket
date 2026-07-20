# Build stage
FROM gradle:8.7-jdk21 AS build
WORKDIR /home/gradle/src
COPY --chown=gradle:gradle . .
RUN gradle build --no-daemon

# Package stage
FROM eclipse-temurin:21-jre-jammy
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system app && useradd --system --gid app --no-create-home app
WORKDIR /app
COPY --from=build /home/gradle/src/build/libs/*.jar app.jar
RUN chown app:app app.jar
USER app
EXPOSE 8083
# Without -XX:MaxRAMPercentage, the JVM sizes its default heap off the HOST's memory (visible
# through /proc/meminfo even inside a cgroup-limited container on older JDKs/runtimes), not the
# container's actual memory limit - reliably OOM-killed the moment the heap grows past whatever
# the container was actually capped at. The shell form (not exec-array JSON) is required so
# $JAVA_OPTS actually expands; `exec` then replaces the shell so SIGTERM still reaches the JVM
# directly instead of the shell absorbing it and the JVM never noticing.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
