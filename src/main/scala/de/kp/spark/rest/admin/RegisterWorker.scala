package de.kp.spark.rest.admin
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

import de.kp.spark.rest.BaseActor
import de.kp.spark.rest.Configuration

import de.kp.spark.rest.model._
import de.kp.spark.rest.redis.RedisCache

import de.kp.spark.rest.spec.FieldBuilder
import scala.collection.mutable.ArrayBuffer

class RegisterWorker extends BaseActor {

  private val (host,port) = Configuration.redis
  protected val cache = new RedisCache(host,port.toInt)
  
  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")
      
      val response = try {
        register(req)
        
      } catch {
        case throwable:Throwable => failure(req,throwable.getMessage)
      }
      
      origin ! response
      context.stop(self)

    }
    
  }

  protected def register(req:ServiceRequest):ServiceResponse = {
    
    val uid = req.data("uid")
    val topic = req.task.split(":")(1)
    
    topic match {
       
      case "event" => {
        
        val fields = new FieldBuilder().build(req,topic)
        cache.addFields(req, fields.toList)
        
        new ServiceResponse(req.service,req.task,Map("uid"-> uid),ResponseStatus.SUCCESS)
      
      }
      case "item" => {
        
        val fields = new FieldBuilder().build(req,topic)
        cache.addFields(req, fields.toList)
        
        new ServiceResponse(req.service,req.task,Map("uid"-> uid),ResponseStatus.SUCCESS)
                
      }        
      case "point" => {
        
        val fields = new FieldBuilder().build(req,topic)
        cache.addFields(req, fields)
        
        new ServiceResponse(req.service,req.task,Map("uid"-> uid),ResponseStatus.SUCCESS)
          
      }
      case "sequence" => {
        
        val fields = new FieldBuilder().build(req,topic)
        cache.addFields(req, fields)
        
        new ServiceResponse(req.service,req.task,Map("uid"-> uid),ResponseStatus.SUCCESS)
          
      }
      case "state" => {
        
        val fields = new FieldBuilder().build(req,topic)
        cache.addFields(req, fields)
        
        new ServiceResponse(req.service,req.task,Map("uid"-> uid),ResponseStatus.SUCCESS)
          
      }
      case "vector" => {
        
        val fields = new FieldBuilder().build(req,topic)
        cache.addFields(req, fields)
        
        new ServiceResponse(req.service,req.task,Map("uid"-> uid),ResponseStatus.SUCCESS)
          
      }
      case _ => {
          
         val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
         throw new Exception(msg)
          
       }

    }
    
  }
  
}