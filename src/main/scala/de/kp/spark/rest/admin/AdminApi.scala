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

import java.util.Date

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.pattern.ask

import akka.util.Timeout

import spray.http.StatusCodes._
import spray.httpx.encoding.Gzip
import spray.httpx.marshalling.Marshaller

import spray.routing.{Directives,HttpService,RequestContext,Route}
import spray.routing.directives.EncodingDirectives
import spray.routing.directives.CachingDirectives

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.spark.rest.{Configuration,RestService}
import de.kp.spark.rest.model._

class AdminApi(host:String,port:Int,system:ActorSystem) extends HttpService with Directives {

  val (duration,retries,time) = Configuration.actor   

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  private val RouteCache = CachingDirectives.routeCache(1000,16,Duration.Inf,Duration("30 min"))
  
  val administrator = system.actorOf(Props(new AdminMaster()), name="AdminMaster")
 
  def start() {
    RestService.start(routes,system,host,port)
  }

  /*
   * The routes defines the different access channels this API supports
   */
  private def routes:Route = {

    /*
     * A 'fields' request supports the retrieval of the field
     * or metadata specificiations that are associated with
     * a certain training task (uid).
     * 
     * The approach actually supported enables the registration
     * of field specifications on a per uid basis, i.e. each
     * task may have its own fields. Requests that have to
     * refer to the same fields must provide the SAME uid
     */
    path("fields" / Segment / Segment) {(service,subject) =>   
	  post {
	    respondWithStatus(OK) {
	      ctx => doFields(ctx,service,subject)
	    }
	  }
    }  ~  
    /*
     * A 'register' request supports the registration of a field
     * or metadata specification that describes the fields used
     * to span the training dataset.
     */
    path("register" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doRegister(ctx,service,subject)
	    }
	  }
    }  ~ 
    /*
     * A 'params' request supports the retrieval of the parameters
     * used for a certain model training task
     */
    path("params" / Segment / Segment) {  (service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doParams(ctx,service,subject)
	    }
	  }
    }  ~ 
    /*
     * A 'status' request supports the retrieval of the status
     * with respect to a certain training task (uid). The latest
     * status or all stati of a certain task are returned.
     */
    path("status" / Segment / Segment) {(service,subject) =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,service,subject)
	    }
	  }
    }  ~ 
    pathPrefix("web") {
      /*
       * 'web' is the prefix for static public content that is
       * served from a web browser and provides a minimalistic
       * web UI for this prediction server
       */
      implicit val actorContext = actorRefFactory
      get {
	    respondWithStatus(OK) {
	      getFromResourceDirectory("public")
	    }
      }
    }
  }

  /**
   * 'fields' and 'register' requests refer to the metadata management of
   * Predictiveworks; for a certain task (uid) and a specific model (name), 
   * a specification of the respective data fields can be registered and 
   * retrieved from a Redis database.
   * 
   * Request parameters for the 'fields' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   */
  private def doFields[T](ctx:RequestContext,service:String,subject:String) = doRequest(ctx,service,"fields")
  /**
   * Request parameters for the 'params' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   */
  private def doParams[T](ctx:RequestContext,service:String,subject:String) = doRequest(ctx,service,"params")
 
  private def doRegister[T](ctx:RequestContext,service:String,subject:String) = {
    
    val task = "register" + ":" + subject
    service match {
      /*
       * Request parameters for the 'register' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * The information element (item) is pre-defined for the Association Analysis
       * engine. This implies, that for registration requests, the following additional
       * parameters have to be provided (the value specifies tha data source field name):
       * 
       * - user (String)
       * - timestamp (String) 
       * - group (String)
       * - item (String)
       * - score (String)
       * 
       * These parameters are used to specify the mapping between the field name used by the
       * Association Analysis engine and the respective field name in the data source.
       * 
       */         
	  case "association" => {
	    
	    val topics = List("item")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
      /*
       * Request parameters for the 'register' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - row (Long)
       * - col (Long)
       * - cat (String)
       * - val (Double)
       * 
       */   
	  case "context" => {
	    
	    val topics = List("point")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
      /*
       * Request parameters for the 'register' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - names (String, comma separated list of feature names)
       * - types (String, comma separated list of feature types)
       * 
       */    
      case "decision" => {
	    
	    val topics = List("feature")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
      }            
      /*
       * Request parameters for the 'register' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - user (String)
       * - timestamp (Long) 
       * - state (String)
       * 
       */   
      case "intent" => {
	    
	    val topics = List("state")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
      /*
       * Request parameters for the 'register' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * The information element, 'state' or 'vector' determines how to proceed:
       * 
       * topic:state
       * 
       * - user (String)
       * - timestamp (Long) 
       * - state (String)
       *  
       * topic:vector
       * 
       * - row (Long)
       * - col (Long)
       * - lbl (String)
       * - val (Double)
       * 
       */    
	  case "outlier" => {
	    
	    val topics = List("state","vector")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
      /*
       * Request parameters for the 'register' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * The information element, 'event' or 'item' determines how to proceed:
       * 
       * topic: event
      * 
       * - user (String)
       * - timestamp (Long)
       * - event (Integer)
       * - item (Integer)
       * - score (Double)
       * 
       * topic: item
       * 
       * - user (String)
       * - timestamp (Long)
       * - group (String)
       * - item (String, comma separated list of Integers)
       * - score (String, comma separated list of Double)
       * 
       */   
	  case "recommendation" => {
	    
	    val topics = List("event","item")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
      /*
       * Request parameters for the 'register' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - user (String)
       * - timestamp (Long)
       * - group (String)
       * - item (Integer)
       * 
       */   
	  case "series" => {
	    
	    val topics = List("sequence")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
      /*
       * Request parameters for the 'register' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * topic: sequence
       * 
       * - user (String)
       * - timestamp (Long)
       * - group (String)
       * - item (Integer)
       *  
       * topic:vector
       * 
       * - row (Long)
       * - col (Long)
       * - lbl (String)
       * - val (Double)
       */  
	  case "similarity" => {
	    
	    val topics = List("sequence","vector")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
	  case "social" => {
	    
	    subject match {
	      /* Not implemented yet */
	      case _ => {}
	      
	    }
	    
	  }
	  case "text" => {
	    
	    subject match {
	      /* Not implemented yet */
	      case _ => {}
	      
	    }
	    
	  }

	  case _ => {}
	  
    }
    
  }
  /**
   * 'status' is an administration request to determine whether a certain data
   * mining task has been finished or not; the only parameter required for status 
   * requests is the unique identifier of a certain task
   * 
   * Request parameters for the 'status' request:
   * 
   * - site (String)
   * - uid (String)
   * 
   */
  private def doStatus[T](ctx:RequestContext,service:String,subject:String) = {
    
    subject match {
      /*
       * Retrieve the 'latest' status information about a certain
       * data mining or model building task.
       */
      case "latest" => doRequest(ctx,service,"status:latest")
      /*
       * Retrieve 'all' stati assigned to a certain data mining
       * or model building task.
       */
      case "all" => doRequest(ctx,service,"status:all")
      
      case _ => {/* do nothing */}
    
    }
  
  }
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String) = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(master(task),request).mapTo[ServiceResponse] 
    ctx.complete(response)
    
  }

  private def getHeaders(ctx:RequestContext):Map[String,String] = {
    
    val httpRequest = ctx.request
    
    /* HTTP header to Map[String,String] */
    val httpHeaders = httpRequest.headers
    
    Map() ++ httpHeaders.map(
      header => (header.name,header.value)
    )
    
  }
 
  private def getBodyAsMap(ctx:RequestContext):Map[String,String] = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    val body = JSON.parseFull(httpEntity.data.asString) match {
      case Some(map) => map
      case None => Map.empty[String,String]
    }
      
    body.asInstanceOf[Map[String,String]]
    
  }
  
  private def getRequest(ctx:RequestContext):Map[String,String] = {

    val headers = getHeaders(ctx)
    val body = getBodyAsMap(ctx)
    
    headers ++ body
    
  }
  
  private def master(task:String):ActorRef = administrator
  
}