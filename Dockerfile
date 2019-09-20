FROM openjdk:8-jre
WORKDIR /
COPY target/kstreams-count-statefulset-1.0.jar /
CMD ["java", "-jar","kstreams-count-statefulset-1.0.jar"]