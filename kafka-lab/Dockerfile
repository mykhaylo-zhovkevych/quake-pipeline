LABEL authors="mykhaylo zhovkevych"
# -- Build
FROM maven:3.9-eclipse-temurin-21 as build
WORKDIR /build

COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q clean package -DskipTests

# -- Runtime
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S app && adduser -S -G app app

COPY --from=build /build/target/kafka-lab.jar app.jar
USER app

ENV KAFKA_BOOTSTRAP=kafka:19092 \
    DB_URL=jdbc:postgresql://postgres:5432/events \
    DB_USER=app \
    DB_PASSWORD=app

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]