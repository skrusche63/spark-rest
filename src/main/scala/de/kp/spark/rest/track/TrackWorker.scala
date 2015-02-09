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
import de.kp.spark.rest.elastic._

import de.kp.spark.rest.redis.RedisCache

import org.elasticsearch.common.xcontent.XContentBuilder

class TrackWorker extends BaseActor {

  private val (host,port) = Configuration.redis
  protected val cache = new RedisCache(host,port.toInt)
  
  def receive = {

    /*
     * A 'track' request generates two responses: an initial one, that informs
     * the sender that the data have been received and a second one that the
     * data have been processed.
     */
    case req:ServiceRequest => {
      /*
       * STEP#1: Prepare initial response 
       */
      val uid = req.data("uid")
      
      val data = Map("uid" -> uid, "message" -> Messages.TRACKED_DATA_RECEIVED(uid))
      val initialResponse = new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)	
      
      val origin = sender
      origin ! initialResponse
      
      /*
       * STEP#2: Prepare final response 
       */
      val finalResponse = try {
        track(req)
      
      } catch {
        case e:Exception => failure(req,e.getMessage)
      }
      
      origin ! finalResponse
      context.stop(self)

    }
  
  }
  
  protected def track(req:ServiceRequest):ServiceResponse = {

   val uid = req.data("uid")
   
   val index   = req.data("index")
   val mapping = req.data("type")
    
   val writer = new ElasticWriter()
        
   val readyToWrite = writer.open(index,mapping)
   if (readyToWrite == false) {
      
     writer.close()
      
     val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
     throw new Exception(msg)
      
   } else {
          
     /*
      * Set status to indicate that the data tracking has started
      */
     cache.addStatus(req,ResponseStatus.TRACKING_STARTED)
 
     req.task.split(":")(1) match {

       case "event" => {
         
         val source = prepareEvent(req)
         writer.writeJSON(index, mapping, source)        
        
       }       
       case "feature" => {
      
         val source = prepareFeature(req)
         writer.writeJSON(index, mapping, source)
         
       }
       case "item" => {
         writer.writeBulkJSON(index, mapping, prepareItem(req))
         
       }      
       case "point" => {
      
         val source = preparePoint(req)
         writer.writeJSON(index, mapping, source)
         
       }
       case "sequence" => {
      
         val source = prepareSequence(req)
         writer.writeJSON(index, mapping, source)
         
       }
       case "state" => {     
         writer.writeJSON(index, mapping, prepareState(req))
         
       }
       case "vector" => {
         /*
          * Writing this source to the respective index throws an
          * exception in case of an error; note, that the writer is
          * automatically closed 
          */
         writer.writeJSON(index, mapping, prepareVector(req))
         
       }
       case _ => {
          
         val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
         throw new Exception(msg)
          
       }
      
     }
 
     /*
      * Set status to indicate that the respective data have
      * been tracked sucessfully
      */
     cache.addStatus(req,ResponseStatus.TRACKING_FINISHED)
     
     val data = Map("uid" -> uid)
     new ServiceResponse(req.service,req.task,data,ResponseStatus.TRACKING_FINISHED)
  
   }
    
  }
  
  protected def prepareEvent(req:ServiceRequest):XContentBuilder = {
    new ElasticEventBuilder().createSourceJSON(req.data)
  }
    
  protected def prepareFeature(req:ServiceRequest):XContentBuilder = {
    new ElasticFeatureBuilder().createSourceJSON(req.data)
  }

  protected def prepareItem(req:ServiceRequest):List[XContentBuilder] = {
    new ElasticItemBuilder().createSourceJSON(req.data)
  }
  
  protected def preparePoint(req:ServiceRequest):XContentBuilder = {
    new ElasticPointBuilder().createSourceJSON(req.data)    
  }
  
  protected def prepareSequence(req:ServiceRequest):XContentBuilder = {
    new ElasticSequenceBuilder().createSourceJSON(req.data)    
  }
    
  protected def prepareState(req:ServiceRequest):XContentBuilder = {
    new ElasticStateBuilder().createSourceJSON(req.data)
  }
  
  protected def prepareVector(req:ServiceRequest):XContentBuilder = {
    new ElasticVectorBuilder().createSourceJSON(req.data)    
  }
 
}