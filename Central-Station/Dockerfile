FROM openjdk:19

WORKDIR /usr/src/myapp

COPY out ./out

VOLUME /bitcask

VOLUME /parquet-files

ENTRYPOINT sleep 60 && java -jar out/artifacts/Central_Station_jar/Central-Station.jar