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

class ParamWorker extends BaseActor {

  private val (host,port) = Configuration.redis
  protected val cache = new RedisCache(host,port.toInt)

  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")
         
      val response = if (cache.paramsExist(req) == false) {           
        failure(req,Messages.TASK_DOES_NOT_EXIST(uid))           

      } else {            
        params(req)
            
      }
           
      origin ! response
      context.stop(self)
      
    }
  
  }

  protected def params(req:ServiceRequest):ServiceResponse = {
    
    val params = Params(cache.params(req))
    
    val uid = req.data("uid")
    val data = Map("uid" -> uid, "response" -> Serializer.serializeParams(params))
                
    new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)	

  }

}