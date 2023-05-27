FROM openjdk:19

WORKDIR /usr/src/myapp

COPY out ./out

ENTRYPOINT sleep 90 && java -jar out/artifacts/Weather_Station_jar/Weather-Station.jar 10
