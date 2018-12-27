package io.redpencil

import org.eclipse.rdf4j.repository.util.RDFInserter
import org.eclipse.rdf4j.repository.RepositoryConnection
import org.eclipse.rdf4j.model.impl.LinkedHashModel
import org.eclipse.rdf4j.model._
import org.eclipse.rdf4j.model.IRI
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.model.vocabulary.XMLSchema
import scala.collection.JavaConversions._
import org.eclipse.rdf4j.rio.ntriples.NTriplesWriter
import java.io.ByteArrayOutputStream



class Handler(con:RepositoryConnection, graph:IRI) extends RDFInserter(con) {
  val BATCH_SIZE=1000
  val cache = new LinkedHashModel();

  def buildQueryFromCache = {
    val baos = new ByteArrayOutputStream()
    val ntriplesWriter = new NTriplesWriter(baos)
    ntriplesWriter.startRDF
    cache.foreach( s => ntriplesWriter.handleStatement(s))
    ntriplesWriter.endRDF
    val statements = baos.toString("UTF8")
    s"""INSERT DATA INTO <${graph.stringValue}> {
      $statements
    }"""
  }
  override def addStatement(subj: Resource, pred: IRI, obj: Value, ctxt: Resource) = {
    cache.add(subj,pred,obj);
    if (cache.size == BATCH_SIZE) {
      con.begin()
      con.prepareUpdate(buildQueryFromCache).execute()
      con.commit()
      cache.clear();
    }
  }
  override def endRDF() ={
    if (cache.size > 0) {
      con.begin()
      con.prepareUpdate(buildQueryFromCache).execute()
      con.commit()
      cache.clear();
    }
  }
}
