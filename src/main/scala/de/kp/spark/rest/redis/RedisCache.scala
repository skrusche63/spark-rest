package de.kp.spark.rest.redis
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

import java.util.Date
import scala.collection.JavaConversions._

import de.kp.spark.rest.model._
import de.kp.spark.rest.model.Serializer

class RedisCache(host:String,port:Int) extends Serializable {

  val client  = RedisClient(host,port)
  val serializer = Serializer
  
  /** 
   *  Add a single field specification that refers to a named
   *  training or model building task
   */
  def addField(req:ServiceRequest,field:Field) {addField(req.data,field)}
  
  def addField(data:Map[String,String],field:Field) {
    
    val k = "fields:" + data("site") + ":" + data("uid") + ":" + data("name") 
    val v = String.format("""%s:%s:%s""",field.name,field.datatype,field.value)
    
    client.rpush(k,v)
    
  }
  
  /** 
   *  Add a list of field specifications that refer to a named
   *  training or model build task
   */  
  def addFields(req:ServiceRequest,fields:List[Field]) {
    for (field <- fields) addField(req,field)
    
  }
  
  def addFields(data:Map[String,String],fields:List[Field]) {
    for (field <- fields) addField(data,field)
    
  }

  /** 
   *  Add a single model parameter that refers to a named
   *  training or model building task
   */
  def addParam(req:ServiceRequest,param:Param) {
    
    val k = "params:" + req.data("site") + ":" + req.data("uid") + ":" + req.data("name") 
    val v = String.format("""%s:%s:%s""",param.name,param.datatype,param.value)
    
    client.rpush(k,v)
    
  }
  /** 
   *  Add a list of model parameters that refer to a named
   *  training or model building task
   */  
  def addParams(req:ServiceRequest,params:List[Param]) {
    for (param <- params) addParam(req,param)
    
  }
  
  def addRequest(req:ServiceRequest) {
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "request:" + req.service
    val v = "" + timestamp + ":" + serializer.serializeRequest(req)
    
    client.lpush(k,v)
    
  }
  
  def addStatus(req:ServiceRequest,status:String) {
   
    /*
     * Request may not have a certain service specified, and for
     * engines that use other engines, the service may change with
     * respect to the original one.
     * 
     * Therefore, status monitoring must not use the service as
     * part of the key
     */
    val (uid,service,task) = (req.data("uid"),req.service,req.task)
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "status:"  + req.data("site") + ":" + uid
    val v = "" + timestamp + ":" + serializer.serializeStatus(Status(service,task,status,timestamp))
    
    client.zadd(k,timestamp,v)
    
  }
  
  def fieldsExist(req:ServiceRequest):Boolean = {

    val k = "fields:" + req.data("site") + ":" + req.data("uid") + ":" + req.data("name") 
    client.exists(k)
    
  }
  
  def paramsExist(req:ServiceRequest):Boolean = {

    val k = "params:" + req.data("site") + ":" + req.data("uid") + ":" + req.data("name") 
    client.exists(k)
    
  }
  
  def statusExists(req:ServiceRequest):Boolean = {

    val k = "status:"  + req.data("site") + ":" + req.data("uid")
    client.exists(k)
    
  }
  
  def fields(req:ServiceRequest):List[Field] = {

    val k = "fields:" + req.data("site") + ":" + req.data("uid") + ":" + req.data("name") 
    val fields = client.lrange(k, 0, -1)

    if (fields.size() == 0) {
      List.empty[Field]
    
    } else {
      
      fields.map(field => {
        
        val Array(name,datatype,value) = field.split(":")
        Field(name,datatype,value)
        
      }).toList
     
    }

  }
  
  def params(req:ServiceRequest):List[Param] = {

    val k = "params:" + req.data("site") + ":" + req.data("uid") + ":" + req.data("name") 
    val params = client.lrange(k, 0, -1)

    if (params.size() == 0) {
      List.empty[Param]
    
    } else {
      
      params.map(param => {
        
        val Array(name,datatype,value) = param.split(":")
        Param(name,datatype,value)
        
      }).toList
     
    }

  }
  
  def requestsTotal(service:String):Long = {

    val k = "request:" + service
    if (client.exists(k)) client.llen(k) else 0
    
  }
 
  def requests(service:String,start:Long,end:Long):List[(Long,ServiceRequest)] = {
    
    val k = "request:" + service
    val requests = client.lrange(k, start, end)
    
    requests.map(request => {
      
      val Array(ts,req) = request.split(":")
      (ts.toLong,serializer.deserializeRequest(req))
      
    }).toList
    
  }
  /**
   * This method is deprecated; the newer status interface is based
   * on the method 'statuses'.
   */
  def status(req:ServiceRequest):Status = {

    val k = "status:"  + req.data("site") + ":" + req.data("uid")
    val data = client.zrange(k, 0, -1)

    if (data.size() == 0) {
      null
    
    } else {
      
      /* Format: timestamp:status */
      val last = data.toList.last
      val Array(timestamp,status) = last.split(":")
      
      serializer.deserializeStatus(status)
      
    }

  }
  
  def statuses(req:ServiceRequest):List[Status] = {
    
    val k = "status:"  + req.data("site") + ":" + req.data("uid")
    val data = client.zrange(k, 0, -1)

    if (data.size() == 0) {
      null
    
    } else {
      
      data.map(record => {
        
        val Array(timestamp,status) = record.split(":")
        serializer.deserializeStatus(status)
        
      }).toList
      
    }
    
  }
}