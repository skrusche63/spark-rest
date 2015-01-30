package de.kp.spark.rest.elastic
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-REST project
* (https://github.com/skrusche63/spark-rest).
* 
* Spark-REST is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-REST is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-REST. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.elasticsearch.node.NodeBuilder._

import org.elasticsearch.action.ActionListener

import org.elasticsearch.action.bulk.BulkResponse

import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.index.IndexRequest.OpType

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import org.elasticsearch.client.Requests

class ElasticWriter {
  /*
   * Create an Elasticsearch node by interacting with
   * the Elasticsearch server on the local machine
   */
  private val node = nodeBuilder().node()
  private val client = node.client()
  
  private val logger = Loggers.getLogger(getClass())
  private var readyToWrite = false
  
  def open(index:String,mapping:String):Boolean = {
        
    try {
      
      val indices = client.admin().indices
      /*
       * Check whether referenced index exists; if index does not
       * exist, through exception
       */
      val existsRes = indices.prepareExists(index).execute().actionGet()            
      if (existsRes.isExists() == false) {
        new Exception("Index '" + index + "' does not exist.")            
      }

      /*
       * Check whether the referenced mapping exists; if mapping
       * does not exist, through exception
       */
      val prepareRes = indices.prepareGetMappings(index).setTypes(mapping).execute().actionGet()
      if (prepareRes.mappings().isEmpty) {
        new Exception("Mapping '" + index + "/" + mapping + "' does not exist.")
      }
      
      readyToWrite = true

    } catch {
      case e:Exception => {
        logger.error(e.getMessage())

      }
       
    } finally {
    }
    
    readyToWrite
    
  }

  def close() {
    if (node != null) node.close()
  }

  def exists(index:String,mapping:String,id:String):Boolean = {
    
    val response = client.prepareGet(index,mapping,id).execute().actionGet()
    if (response.isExists()) true else false

  }
   
  def writeJSON(index:String,mapping:String,source:XContentBuilder):Boolean = {
    
    if (readyToWrite == false) return false
    /*
     * The OpType INDEX (other than CREATE) ensures that the document is
     * 'updated' which means an existing document is replaced and reindexed
     */
    client.prepareIndex(index, mapping).setSource(source).setRefresh(true).setOpType(OpType.INDEX)
      .execute(new ActionListener[IndexResponse]() {
        override def onResponse(response:IndexResponse) {
          /*
           * Registration of provided source successfully performed; no further
           * action, just logging this event
           */
          val msg = String.format("""Successful registration for: %s""", source.toString)
          logger.info(msg)
        
        }      

        override def onFailure(t:Throwable) {
	      /*
	       * In case of failure, we expect one or both of the following causes:
	       * the index and / or the respective mapping may not exists
	       */
          val msg = String.format("""Failed to register %s""", source.toString)
          logger.info(msg,t)
	      
          close()
          throw new Exception(msg)
	    
        }
        
      })
      
    true
  
  }
  
  def writeJSON(index:String,mapping:String,id:String,source:XContentBuilder):Boolean = {
    
    if (readyToWrite == false) return false
    /*
     * The OpType INDEX (other than CREATE) ensures that the document is
     * 'updated' which means an existing document is replaced and reindexed
     */
    client.prepareIndex(index,mapping,id).setSource(source).setRefresh(true).setOpType(OpType.INDEX)
      .execute(new ActionListener[IndexResponse]() {
        override def onResponse(response:IndexResponse) {
          /*
           * Registration of provided source successfully performed; no further
           * action, just logging this event
           */
          val msg = String.format("""Successful registration for: %s""", source.toString)
          logger.info(msg)
        
        }      

        override def onFailure(t:Throwable) {
	      /*
	       * In case of failure, we expect one or both of the following causes:
	       * the index and / or the respective mapping may not exists
	       */
          val msg = String.format("""Failed to register %s""", source.toString)
          logger.info(msg,t)
	      
          close()
          throw new Exception(msg)
	    
        }
        
      })
      
    true
  
  }
 
  def writeBulk(index:String,mapping:String,sources:List[java.util.Map[String,Object]]):Boolean = {
     
    if (readyToWrite == false) return false
    
    /*
     * Prepare bulk request and fill with sources
     */
    val bulkRequest = client.prepareBulk()
    for (source <- sources) {
      
      /* Convert source to content */
      val content = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)
      content.map(source)

      bulkRequest.add(client.prepareIndex(index, mapping).setSource(content).setRefresh(true).setOpType(OpType.INDEX))
      
    }
    
    bulkRequest.execute(new ActionListener[BulkResponse](){
      override def onResponse(response:BulkResponse) {

        if (response.hasFailures()) {
          
          val msg = String.format("""Failed to register data for %s/%s""",index,mapping)
          logger.error(msg, response.buildFailureMessage())
                
        } else {
          
          val msg = "Successful registration of bulk sources."
          logger.info(msg)
          
        }        
      
      }
       
      override def onFailure(t:Throwable) {
	    /*
	     * In case of failure, we expect one or both of the following causes:
	     * the index and / or the respective mapping may not exists
	     */
        val msg = "Failed to register bulk of sources."
        logger.info(msg,t)
	      
        close()
        throw new Exception(msg)
	    
      }
      
    })
    
    true
  
  }
  
  def writeBulkJSON(index:String,mapping:String,sources:List[XContentBuilder]):Boolean = {
     
    if (readyToWrite == false) return false
    
    /*
     * Prepare bulk request and fill with sources
     */
    val bulkRequest = client.prepareBulk()
    for (source <- sources) {
      bulkRequest.add(client.prepareIndex(index, mapping).setSource(source).setRefresh(true).setOpType(OpType.INDEX))
    }
    
    bulkRequest.execute(new ActionListener[BulkResponse](){
      override def onResponse(response:BulkResponse) {

        if (response.hasFailures()) {
          
          val msg = String.format("""Failed to register data for %s/%s""",index,mapping)
          logger.error(msg, response.buildFailureMessage())
                
        } else {
          
          val msg = "Successful registration of bulk sources."
          logger.info(msg)
          
        }        
      
      }
       
      override def onFailure(t:Throwable) {
	    /*
	     * In case of failure, we expect one or both of the following causes:
	     * the index and / or the respective mapping may not exists
	     */
        val msg = "Failed to register bulk of sources."
        logger.info(msg,t)
	      
        close()
        throw new Exception(msg)
	    
      }
      
    })
    
    true
  
  }
  
  def writeBulkJSON(index:String,mapping:String,ids:List[String],sources:List[XContentBuilder]):Boolean = {
     
    if (readyToWrite == false) return false
    
    val zipped = ids.zip(sources)

    /*
     * Prepare bulk request and fill with sources
     */
    val bulkRequest = client.prepareBulk()
    for ((id,source) <- zipped) {
      bulkRequest.add(client.prepareIndex(index,mapping,id).setSource(source).setRefresh(true).setOpType(OpType.INDEX))
    }
    
    bulkRequest.execute(new ActionListener[BulkResponse](){
      override def onResponse(response:BulkResponse) {

        if (response.hasFailures()) {
          
          val msg = String.format("""Failed to register data for %s/%s""",index,mapping)
          logger.error(msg, response.buildFailureMessage())
                
        } else {
          
          val msg = "Successful registration of bulk sources."
          logger.info(msg)
          
        }        
      
      }
       
      override def onFailure(t:Throwable) {
	    /*
	     * In case of failure, we expect one or both of the following causes:
	     * the index and / or the respective mapping may not exists
	     */
        val msg = "Failed to register bulk of sources."
        logger.info(msg,t)
	      
        close()
        throw new Exception(msg)
	    
      }
      
    })
    
    true
  
  }

}