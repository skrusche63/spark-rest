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

class TrackApi(host:String,port:Int,system:ActorSystem) extends HttpService with Directives {

  val (duration,retries,time) = Configuration.actor   

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  private val RouteCache = CachingDirectives.routeCache(1000,16,Duration.Inf,Duration("30 min"))
  
  val indexer = system.actorOf(Props(new IndexMaster()), name="IndexMaster")
  val tracker = system.actorOf(Props(new TrackMaster()), name="TrackMaster")
 
  def start() {
    RestService.start(routes,system,host,port)
  }

  private def routes:Route = {

    /*
     * 'index' and 'track' requests refer to the tracking functionality of
     * Predictiveworks; while 'index' prepares a certain Elasticsearch index, 
     * 'track' is used to gather training data.
     */
    path("index" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doIndex(ctx,service,subject)
	    }
	  }
    }  ~ 
    path("track" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrack(ctx, service, subject)
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
   * 'index' & 'track' requests support data registration in an Elasticsearch index
   */   
  private def doIndex[T](ctx:RequestContext,service:String,subject:String) = {

    val task = "index" + ":" + subject   
    service match {
      /*
       * Request parameters for the 'index' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - index (String)
       * - type (String)
       * 
       */
 	  case "association" => {
	    
	    val topics = List("item","rule")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
      /*
       * Request parameters for the 'index' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - index (String)
       * - type (String)
       * 
       */
	  case "context" => {
	    
	    val topics = List("point")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
      /*
       * Request parameters for the 'index' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - index (String)
       * - type (String)
       * 
       */  
      case "decision" => {
	    
	    val topics = List("point")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
        
      }	      
      /*
       * Request parameters for the 'index' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - index (String)
       * - type (String)
       * 
       */
      case "intent" => {
	    
	    val topics = List("state")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
      /*
       * Request parameters for the 'index' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - index (String)
       * - type (String)
       * 
       */
      case "outlier" => {
	    
	    val topics = List("state","vector")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
      /*
       * Request parameters for the 'index' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - index (String)
       * - type (String)
       * 
       */
	  case "recommendation" => {
	    
	    val topics = List("event","item")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
      /*
       * Request parameters for the 'index' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - index (String)
       * - type (String)
       * 
       */
	  case "series" => {
	    
	    val topics = List("sequence","rule")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
      /*
       * Request parameters for the 'index' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - index (String)
       * - type (String)
       * 
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
  private def doTrack[T](ctx:RequestContext,service:String,subject:String) = {

    val task = "track" + ":" + subject
    service match {

      /*
       * Request parameters for the 'track' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - index (String)
       * - type (String)
       * 
       * The information element (item) is pre-defined for the Association Analysis
       * engine. This implies, that for tracking requests, the following additional
       * parameters have to be provided:
       * 
       * - user (String)
       * - timestamp (Long)
       * - group (String)
       * - item (String, comma separated list of Integers)
       * - score (String, comma separated list of Double)
       * 
       */   
	  case "association" => {
	    
	    val topics = List("item")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	

	  }
      /*
       * Request parameters for the 'track' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - source (String)
       * - type (String)
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
       * Request parameters for the 'track' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - source (String)
       * - type (String)
       * 
       * - row (Long)
       * - col (Long)
       * - cat (String)
       * - val (Double)
       * 
       */   
      case "decision" => {
	    
	    val topics = List("point")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	

      }      
      /*
       * Request parameters for the 'track' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - source (String)
       * - type (String)
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
       * Request parameters for the 'track' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - source (String)
       * - type (String)
       * 
       * The information element, 'state' or 'vector' determines how to proceed:
       * 
       * topic: product
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
       * Request parameters for the 'track' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - source (String)
       * - type (String)
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
       * Request parameters for the 'track' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - source (String)
       * - type (String)
       * 
       * The information element (sequence) is pre-defined for the Series Analysis
       * engine. This implies, that for tracking requests, the following additional
       * parameters have to be provided:
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
       * Request parameters for the 'track' request:
       * 
       * - site (String)
       * - uid (String)
       * - name (String)
       * 
       * - source (String)
       * - type (String)
       * 
       * The information element, 'sequence' or 'vector' determines how to proceed:
       * 
       * topic:sequence
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
       * 
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
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String="train") = {
     
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
  
  private def master(task:String):ActorRef = {
    
    val req = task.split(":")(0)   
    req match {
      
      case "index" => indexer
      case "track" => tracker
      
      case _ => null
      
    }
  }
}