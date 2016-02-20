FROM java:8
COPY target /usr/src/eventstore
WORKDIR /usr/src/eventstore
EXPOSE 80
CMD ["java", "-DEVENTSTORE_HTTP_PORT=80", "-jar", "eventstore-1.0-SNAPSHOT-fat.jar"]
