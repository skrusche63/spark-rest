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

import de.kp.spark.rest.actor.BaseActor
import de.kp.spark.rest.Configuration

import de.kp.spark.rest.model._
import de.kp.spark.rest.redis.RedisCache

class StatusWorker extends BaseActor {

  private val (host,port) = Configuration.redis
  protected val cache = new RedisCache(host,port.toInt)

  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")
         
      val response = if (cache.statusExists(req) == false) {           
        failure(req,Messages.TASK_DOES_NOT_EXIST(uid))           

      } else {     
        
        try {
          doStatus(req)
        
        } catch {
          case e:Exception => failure(req,e.getMessage)
        }    
      
      }
           
      origin ! response
      context.stop(self)
      
    }
  
  }

  protected def doStatus(req:ServiceRequest):ServiceResponse = {

    val task = req.task
    val uid = req.data("uid")
    
    if (task == "status") {
      /*
       * This is case is supported to be compliant to 
       * previous versions of the status management
       */
      val data = Map("uid" -> uid)               
      return new ServiceResponse(req.service,req.task,data,Serializer.serializeStatus(cache.status(req)))

    }
    /*
     * The actual status management supports the following tasks:
     * 
     * 1) status:latest & 2) status:all
     * 
     */
    val topic = task.split(":")(1)
    if (topic == "latest") {
      
      val latest = cache.statuses(req).last
      val result = StatusList(List(latest))
      
      val data = Map("uid" -> uid,"status" -> Serializer.serializeStatusList(result))               
      return new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)	
    
    } else if (topic == "all") {
      
      val statuses = cache.statuses(req)
      val result = StatusList(statuses)
      
      val data = Map("uid" -> uid,"status" -> Serializer.serializeStatusList(result))               
      return new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)	
     
    } else {
      throw new Exception("This status request type is not supported.")
    }
    
  }

}