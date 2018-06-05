# mandaten-import-service
Microservice that imports a ttl file and overwrites the existing specified graph with its contents. Runs a cron service to run the import at a set interval.
The service scans a directory and writes the latest turtle file in that directory to the application graph. See [import.sh](import.sh) for more information.

## Installation
To add the service to your stack, add the following to `docker-compose.yml`:

```
services:
  import:
    image: lblod/mandaten-import-service:0.1.0
    volumes:
      - /path/to/imports:/data/imports
```

## Configuration
The service can be configured through the following environment variables:
* `DEFAULT_GRAPH`: graph to write to, by default `http://mu.semte.ch/application`
* `IMPORT_DIR`: path to monitor, by default `/data/imports`
* `SPARQL_ENDPOINT`: sparql endpoint to use
* `CLEAR_ENDPOINT`: endpoint to call to clear cache
* `CRON_PATTERN`: cron pattern (e.g `* * * * *`), when to run the import
