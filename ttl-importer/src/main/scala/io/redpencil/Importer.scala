package io.redpencil

import org.eclipse.rdf4j.query.QueryLanguage
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository
import org.eclipse.rdf4j.rio.RDFFormat
import scala.collection.JavaConverters._
import org.eclipse.rdf4j.RDF4JException
import org.eclipse.rdf4j.rio.Rio
import java.io.FileInputStream
import java.io.InputStreamReader
import java.util.UUID
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Importer {
  // argument parsing via https://stackoverflow.com/questions/2315912/best-way-to-parse-command-line-parameters#3183991
   val usage = """
    Usage: importer [--endpoint http://localhost:8890/sparql] [--file filename.ttl] [--graph http://your-graph/] [--keep-data] [--post-process-queries /full/path/to/queries/]
  """
  def uuid = UUID.randomUUID

  def listSparqlFiles(path: String) = {
    val files = ArrayBuffer.empty[Path]
    val root = Paths.get(path)
    Files.walkFileTree(root, new SimpleFileVisitor[Path] {
                         override def visitFile(file: Path, attrs: BasicFileAttributes) = {
                           if (file.getFileName.toString.endsWith(".rq")) {
                             files += file
                         }
                         FileVisitResult.CONTINUE
                       }
                     })
    files
  }

  def main(args: Array[String]) {
    val arglist = args.toList
    type OptionMap = Map[Symbol, String]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--endpoint" :: value :: tail =>
          nextOption(map ++ Map('endpoint -> value), tail)
        case "--file" :: value :: tail =>
          nextOption(map ++ Map('file -> value), tail)
        case "--graph" :: value :: tail =>
          nextOption(map ++ Map('graph -> value), tail)
        case "--keep-data" :: tail =>
          nextOption(map ++ Map('keepData -> "keepData" ), tail)
        case "--post-process-queries" :: value :: tail =>
          nextOption(map ++ Map('queryFolder -> value), tail)
      }
    }

    def executeWithRetry[T](callback: () => T, currentAttemptNumber: Int, maxAttempts: Int, sleep: Int): T = {
      try {
        println("Executing call")
        callback()
      }
      catch {
        case e:Exception => {
          println(s"Error executing call")
          e.printStackTrace()
          if(currentAttemptNumber >= maxAttempts) {
            println("Max attempts of import reached, giving up...")
            throw new Exception("Max attempts reached")
          }
          else {
            val nextSleepTime = (sleep * scala.util.Random.nextFloat).toLong
            println(s"Attempt $currentAttemptNumber/$maxAttempts failed, sleeping $nextSleepTime seconds")
            Thread.sleep(nextSleepTime * 1000)
            executeWithRetry(callback, currentAttemptNumber + 1, maxAttempts, sleep);
          }
        };
     }
    }

    def importFile(endpoint: String, filePath: String, graph: String, tempGraph: String): Unit = {
      println("Start file import")
      println("Initializing repo")
      val repo = new SPARQLRepository(endpoint)
      repo.initialize()

      val file = new InputStreamReader(new FileInputStream(filePath),"utf-8")

      println("Getting connection")
      val con = repo.getConnection()

      val tempGraphUri = con.getValueFactory.createIRI(tempGraph)

      println(s"Loading data into $tempGraphUri")

      val parser = Rio.createParser(RDFFormat.TURTLE)
      val handler = new Handler(con, tempGraphUri)
      parser.setRDFHandler(handler)
      parser.parse(file, tempGraph)

      println(s"Loading data into $tempGraphUri OK")
    }

    def moveData(endpoint: String, graph: String, tempGraph: String, keepData: Boolean): Unit = {
      println("Start move data")
      val query =
        if (keepData) {
        s"ADD GRAPH <$tempGraph> TO <$graph>; DROP SILENT GRAPH <$tempGraph>"
      }
      else {
        s"MOVE GRAPH <$tempGraph> TO <$graph>"
      }

      println("Initializing repo")
      val repo = new SPARQLRepository(endpoint)
      repo.initialize()
      println("Getting connection")
      val con = repo.getConnection()

      println(s"MOVE OR ADD FROM $tempGraph to $graph")
      con.prepareUpdate(QueryLanguage.SPARQL, query).execute()
      println(s"MOVE OR ADD FROM $tempGraph to $graph OK")
    }

    def postProcessData(endpoint: String, queryFolder: String): Unit = {
      println("Start postprocess")
      println("Initializing repo")
      val repo = new SPARQLRepository(endpoint)
      repo.initialize()
      println("Getting connection")
      val con = repo.getConnection()

       val path = queryFolder
      val queries = listSparqlFiles(queryFolder)

      queries.foreach( (path: Path ) => {
         val query = Source.fromFile(path.toString).mkString
         println(s"Running query from $path")
         println(s"Query: $query")
         con.prepareUpdate(QueryLanguage.SPARQL, query).execute()
      } )
    }

    def cleanUpTempGraph(endpoint: String, tempGraph: String){
      println("Start cleanup")
      println("Initializing repo")
      val repo = new SPARQLRepository(endpoint)
      repo.initialize()
      println("Getting connection")
      val con = repo.getConnection()
      val cleanupQuery = "DROP SILENT GRAPH <$tempGraph>"
      con.prepareUpdate(QueryLanguage.SPARQL, cleanupQuery).execute()
      println(s"Query $cleanupQuery seemed fine")
    }

    def importData() : Unit = {
      val options = nextOption(Map(),arglist)
      val endpoint = options.getOrElse('endpoint, "")
      val filePath = options.getOrElse('file,"")
      val graph = options.getOrElse('graph,"")
      val tempGraph = s"http://data.lblod.info/temp/$uuid"
      val keepData = options.contains('keepData)
      val queryFolder = options.getOrElse('queryFolder,"")

      try {
        println("Starting import")
        executeWithRetry(() => importFile(endpoint, filePath, graph, tempGraph), 0, 20, 120)
        executeWithRetry(() => moveData(endpoint, graph, tempGraph, keepData), 0, 20, 120)

        if(options.contains('queryFolder)){
          executeWithRetry(() => postProcessData(endpoint, queryFolder), 0, 20, 120)
        }
        print("Import seems ok...")
      }
      catch {
        case e:Throwable => {
          println("Import failed...")
          e.printStackTrace;
          executeWithRetry(() => cleanUpTempGraph(endpoint, tempGraph), 0, 20, 120)
          throw new Exception("Import failed")
        }
      }
    }

    try {
      importData()
    }
    catch {
      case e:scala.MatchError => {println(usage); System.exit(-1) }
      case e:Throwable => { e.printStackTrace; System.exit(1) }
    }
  }
}