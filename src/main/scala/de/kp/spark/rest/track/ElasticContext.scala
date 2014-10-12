package de.kp.spark.rest.track
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

import java.util.Random

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.ForkJoinPool

import org.elasticsearch.node.NodeBuilder._

import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.index.IndexRequest.OpType

import org.elasticsearch.indices.IndexAlreadyExistsException

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import org.elasticsearch.client.Requests
import scala.collection.JavaConversions._

class ElasticContext {

  private val node = nodeBuilder().node()
  private val client = node.client()
  
  private val random = new Random()
  private val logger = Loggers.getLogger(getClass())
  
  private val commonPool = new ForkJoinPool()
  private val indexCreationLock = new ReentrantLock()

  private val DEFAULT_HEALTH_REQUEST_TIMEOUT:String = "30s"
  
  def register(index:String,mapping:String,builder:XContentBuilder,source:java.util.Map[String,Object]) {
    
    val responseListener = new ListenerUtils.OnIndexResponseListener[IndexResponse]() {
      override def onResponse(response:IndexResponse) {
        /*
         * Registration of provided source successfully performed; no further
         * action, just logging this event
         */
        val msg = String.format("""Successful registration for: %s""", source.toString)
        logger.info(msg)
        
      }      
    }
		
	val failureListener = new ListenerUtils.OnFailureListener() {
      override def onFailure(t:Throwable) {
	    /*
	     * In case of failure, we expect one or both of the following causes:
	     * the index and / or the respective mapping may not exists
	     */
	    sleep(t)
	    println("Failure: " + t.getMessage)
	    indexExists(index,mapping,builder,source)
	      
	  }
	}
        
    /* Update index operation */
    val content = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)
    content.map(source)
	
    client.prepareIndex(index, mapping).setSource(content).setRefresh(true).setOpType(OpType.INDEX)
      .execute(ListenerUtils.onIndex(responseListener, failureListener))
    
  }
  
  def shutdown() {
    if (node != null) node.close()
  }
  
  private def indexExists(index:String,mapping:String,builder:XContentBuilder,source:java.util.Map[String,Object]) {
        
    try {
      
      indexCreationLock.lock()
            
      val response = client.admin().indices().prepareExists(index).execute().actionGet()            
      if (response.isExists()) {
        createMapping(index,mapping,builder,source)
            
      } else {
        createIndex(index,mapping,builder,source)
            
      }
    } catch {
      case e:Exception => {
        /*
         * In case of an exception retry to register the respective source
         */
        sleep(e)
        fork(new Runnable() {
          override def run() {
			register(index,mapping,builder,source)
		  }
        })            

      }
       
    } finally {
     indexCreationLock.unlock()
    }
    
  }
  
  private def createIndex(index:String,mapping:String,builder:XContentBuilder,source:java.util.Map[String,Object]) {
    
    try {
      
      val response = client.admin().indices().prepareCreate(index).execute().actionGet()            
      if (response.isAcknowledged()) {
        createMapping(index,mapping,builder,source)
            
      } else {
        new Exception("Failed to create " + index)
            
      }
    
    } catch {
      
      case e:IndexAlreadyExistsException => indexExists(index,mapping,builder,source)
        
      case e:Exception => {
          
        sleep(e)
        fork(new Runnable() {
          override def run() {
			register(index,mapping,builder,source)
		  }
        })            

      }
    
    }
  
  }
  
  private def createMapping(index:String,mapping:String,builder:XContentBuilder,source:java.util.Map[String,Object]) {

    try {
        
      val timeout = DEFAULT_HEALTH_REQUEST_TIMEOUT
      val healthResponse = client.admin().cluster().prepareHealth(index)
                             .setWaitForYellowStatus()
                             .setTimeout(timeout)
                             .execute().actionGet()
                             
            
      if (healthResponse.isTimedOut()) {
        new Exception("Failed to create index: " + index + "/" + mapping)
      }

      val mappingResponse = client.admin().indices().preparePutMapping(index).setType(mapping).setSource(builder)
                              .execute().actionGet()
            
      if (mappingResponse.isAcknowledged()) {
        fork(new Runnable() {
          override def run() {
			register(index,mapping,builder,source)
		  }
        })            

      } else {
        new Exception("Failed to create mapping for " + index + "/" + mapping)
            
      }
        
    } catch {
      case e:Exception => logger.debug(e.getMessage)
      
    }
    
  }

  private def fork(task:Runnable) {
    commonPool.execute(task)        
  }
    
  private def sleep(t:Throwable) {
        
    val waitTime = random.nextInt(2500) + 500
    if (logger.isDebugEnabled()) {
      
      val msg = String.format("""Waiting for %s ms and retrying... The cause is: %s""",waitTime.toString,t.getMessage)
      logger.debug(msg,t)
    }
        
    try {
       Thread.sleep(waitTime)
        
    } catch {
      case e1:InterruptedException => {}
        
    }
    
  }
  
}