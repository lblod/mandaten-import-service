# mandaten-import-service
Microservice that imports a ttl file and overwrites the existing specified graph with its contents. Runs a cron service to run the import at a set interval.
The service scans a directory and writes the latest turtle file in that directory to the application graph. See [import.sh](import.sh) for more information.

Optionally you can provide some sparql queries to run after the import has finished. These should be mounted in `/data/queries` and have a `.rq` extension.
## Installation
To add the service to your stack, add the following to `docker-compose.yml`:

```
services:
  import:
    image: lblod/mandaten-import-service:0.2.0
    volumes:
      - /path/to/imports:/data/imports
      - /path/to/queries:/data/queries
```

## Configuration
The service can be configured through the following environment variables:
* `DEFAULT_GRAPH`: graph to write to, by default `http://mu.semte.ch/application`
* `IMPORT_DIR`: path to monitor, by default `/data/imports`
* `SPARQL_ENDPOINT`: sparql endpoint to use
* `CLEAR_ENDPOINT`: endpoint to call to clear cache
* `CRON_PATTERN`: cron pattern (e.g `* * * * *`), when to run the import
* `KEEP_DATA`: if set keep data in destination graph, if not overwrite the graph (the default)
