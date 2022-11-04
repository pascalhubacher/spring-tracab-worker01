FROM openjdk:20-jdk-slim-buster

ENV DATAPLATFORM_IP=localhost

WORKDIR /app
COPY ./target/spring-tracab-worker01-0.0.1-SNAPSHOT.jar /app

EXPOSE 8080

CMD ["java", "-jar", "spring-tracab-worker01-0.0.1-SNAPSHOT.jar"]