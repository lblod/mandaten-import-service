package io.redpencil

import org.eclipse.rdf4j.query.QueryLanguage
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository
import org.eclipse.rdf4j.rio.RDFFormat;
import scala.collection.JavaConverters._
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.rio.Rio
import java.io.FileInputStream;
import java.util.UUID;

object Importer {
  // argument parsing via https://stackoverflow.com/questions/2315912/best-way-to-parse-command-line-parameters#3183991
   val usage = """
    Usage: importer [--endpoint http://localhost:8890/sparql] [--file filename.ttl] [--graph http://your-graph/]
  """
  def uuid = UUID.randomUUID

  def main(args: Array[String]) {
    if (args.length == 0) println(usage)
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
      }
    }
    val options = nextOption(Map(),arglist)
    val repo = new SPARQLRepository(options.getOrElse('endpoint, ""))
    repo.initialize()
    val file = new FileInputStream(options.getOrElse('file,""));
    val baseURI = "http://example.org/example/local";
    try {
      val con = repo.getConnection();
      val tempGraph = s"http://data.lblod.info/temp/$uuid"
      val graph = options.getOrElse('graph,"")

      val parser = Rio.createParser(RDFFormat.TURTLE)
      val handler = new Handler(con, con.getValueFactory.createIRI(tempGraph))
      parser.setRDFHandler(handler)
      parser.parse(file, tempGraph)
      val query = s"MOVE GRAPH <$tempGraph> TO <$graph>"
      con.prepareUpdate(QueryLanguage.SPARQL, query).execute();
    }
    catch {
      case e => { println(e); e.printStackTrace; System.exit(1) }
    }
  }
}

