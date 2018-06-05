package io.redpencil

import org.eclipse.rdf4j.repository.util.RDFInserter
import org.eclipse.rdf4j.repository.RepositoryConnection
import org.eclipse.rdf4j.model.impl.LinkedHashModel
import org.eclipse.rdf4j.model._
import org.eclipse.rdf4j.model.IRI
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.model.vocabulary.XMLSchema
import scala.collection.JavaConversions._


class Handler(con:RepositoryConnection, graph:IRI) extends RDFInserter(con) {
  val BATCH_SIZE=1000
  val cache = new LinkedHashModel();
  def valueToSPARQL(value:Value) = {
    value match {
      case v:Resource => s"<${v.stringValue}>"
      case v:Literal => {
        v.getDatatype match {
          case RDF.LANGSTRING => s""""${v.stringValue}"@${v.getLanguage.get}"""
          case XMLSchema.STRING => s""""${v.stringValue}""""
          case _ => s""""${v.stringValue}"^^<${v.getDatatype}>"""
        }
      }
    }
  }
  def buildQueryFromCache = {
    val statements = cache.map( (row:Statement) => {
                                 s"${valueToSPARQL(row.getSubject)} ${valueToSPARQL(row.getPredicate)} ${valueToSPARQL(row.getObject)}"
                               }).mkString(".\n" )
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
      con.add(cache, graph)
      cache.clear();
    }
  }
}
