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

import de.kp.spark.rest.Configuration
import de.kp.spark.rest.BaseActor

import de.kp.spark.rest.model._
import de.kp.spark.rest.redis.RedisCache

import de.kp.spark.rest.elastic.{ElasticBuilderFactory => EBF, ElasticIndexer}
import de.kp.spark.rest.spec.FieldBuilder

class IndexWorker extends BaseActor {

  private val (host,port) = Configuration.redis
  protected val cache = new RedisCache(host,port.toInt)
  
  def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data("uid")
      val origin = sender

      try {
        /*
         * Retrieve the field specification for features from the request 
         * as two lists, names & types; this is only required for dynamic
         * field specifications 
         */
        val (names,types) = getSpec(req)
 
        val index   = req.data("index")
        val mapping = req.data("type")
    
        val topic = getTopic(req)
        
        val builder = EBF.getBuilder(topic,mapping,names,types)
        val indexer = new ElasticIndexer()
    
        indexer.create(index,mapping,builder)
        indexer.close()
        
        /*
         * Raw data that are ingested by the tracking functionality do not have
         * to be specified by a field or metadata specification; we therefore
         * add the field specification here as an internal feature
         */        
        val fields = new FieldBuilder().build(req,topic)
        if (fields.isEmpty == false) cache.addFields(req, fields.toList)
        
        val data = Map("uid" -> uid, "message" -> Messages.SEARCH_INDEX_CREATED(uid))
        val response = new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)	
      
        origin ! response
      
      } catch {
        
        case e:Exception => {
          
          log.error(e, e.getMessage())
      
          val data = Map("uid" -> uid, "message" -> e.getMessage())
          val response = new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
      
          origin ! response
          
        }
      
      } finally {
        
        context.stop(self)

      }
    
    }
    
  }
  
  protected def getSpec(req:ServiceRequest):(List[String],List[String]) = {
   
    val Array(task,topic) = req.task.split(":")
    topic match {
      
      case "feature" => {
    
        val names = req.data("names").split(",").toList
        val types = req.data("types").split(",").toList
    
        (names,types)
        
      }
      case _ => (List.empty[String],List.empty[String])

    }
    
  }
  
  protected def getTopic(req:ServiceRequest):String = {
   
    val candidate = req.task.split(":")(1)
    val topic = Topics.get(candidate)
    
    topic
    
  }
  
}