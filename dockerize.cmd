docker build -t spring-tracab-worker01:0.0.1 -t spring-tracab-worker01:latest .

REM docker run --env DATAPLATFORM_IP=localhost -it -p 8080:8080 spring-tracab-worker01:latest