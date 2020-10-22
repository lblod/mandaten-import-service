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

    def importWithRetry(callback: () => Unit, currentAttemptNumber: Int, maxAttempts: Int, sleep: Int) : Unit = {
      try {
        println("Executing import call")
        callback()
        println("Import call seems to be successful")
      }
      catch {
        case e:Exception => {
          println(s"Error importing file")
          e.printStackTrace()
          if(currentAttemptNumber >= maxAttempts) {
            println("Exceeded max attemps executing import, giving up...")
            throw new Exception("Max attempts of import reached")
          }
          else {
            val nextSleepTime = (sleep * scala.util.Random.nextFloat).toLong
            println(s"Attempt $currentAttemptNumber failed, sleeping $nextSleepTime seconds")
            Thread.sleep(nextSleepTime * 1000)
            importWithRetry(callback, currentAttemptNumber + 1, maxAttempts, sleep);
          }
        };
     }
    }

    def importData() : Unit = {
      val options = nextOption(Map(),arglist)
      val repo = new SPARQLRepository(options.getOrElse('endpoint, ""))
      repo.initialize()
      val file = new InputStreamReader(new FileInputStream(options.getOrElse('file,"")),"utf-8")
      val baseURI = "http://example.org/example/local"
      val con = repo.getConnection();
      val tempGraph = s"http://data.lblod.info/temp/$uuid"
      val graph = options.getOrElse('graph,"")
      val parser = Rio.createParser(RDFFormat.TURTLE)
      val handler = new Handler(con, con.getValueFactory.createIRI(tempGraph))
      parser.setRDFHandler(handler)
      parser.parse(file, tempGraph)
      val query =
        if (options.contains('keepData)) {
        s"ADD GRAPH <$tempGraph> TO <$graph>; DROP SILENT GRAPH <$tempGraph>"
      }
      else {
        s"MOVE GRAPH <$tempGraph> TO <$graph>"
      }
      con.prepareUpdate(QueryLanguage.SPARQL, query).execute()
      if (options.contains('queryFolder)) {
        val path = options.get('queryFolder)
        val queries = listSparqlFiles(options.getOrElse('queryFolder,""))

        queries.foreach( (path: Path ) => {
           val query = Source.fromFile(path.toString).mkString
           println(s"running query from $path")
           con.prepareUpdate(QueryLanguage.SPARQL, query).execute()
        } )
      }
    }

    try {
      println("Starting import")
      importWithRetry(importData, 0, 15, 60)
      print("Import seems ok...")
    }
    catch {
      case e:scala.MatchError => {println(usage); System.exit(-1) }
      case e:Throwable => { e.printStackTrace; System.exit(1) }
    }
  }
}
