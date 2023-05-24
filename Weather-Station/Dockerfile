FROM openjdk:19

WORKDIR /usr/src/myapp

COPY out ./out

EXPOSE 8080

CMD ["java", "-jar", "out/artifacts/Weather_Station_jar/Weather-Station.jar","10"]
