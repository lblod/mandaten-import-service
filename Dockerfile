FROM hseeberger/scala-sbt:8u171_2.12.6_1.1.5
COPY import.sh /usr/local/bin/
COPY ttl-importer/ /tmp/ttl-importer/
ENV IMPORT_DIR "/data/imports"
ENV SPARQL_ENDPOINT "http://database:8890/sparql"
ENV DEFAULT_GRAPH "http://mu.semte.ch/application"
ENV CLEAR_ENDPOINT "http://cache/clear"
RUN apt-get -y update && apt-get -y install cron
RUN cd /tmp/ttl-importer  && sbt assembly && mv target/scala-2.12/ttl-importer-assembly-0.1.0-SNAPSHOT.jar /usr/local/bin/import.jar
ADD crontab /etc/crontab
RUN chmod 0600 /etc/crontab
CMD ["cron", "-f", "-L 7"]