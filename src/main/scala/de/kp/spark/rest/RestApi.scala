package de.kp.spark.rest
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

import de.kp.spark.rest.actor.MasterActor
import de.kp.spark.rest.cache.ActorMonitor

import de.kp.spark.rest.model._

class RestApi(host:String,port:Int,system:ActorSystem) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  val (heartbeat,time) = Configuration.actor      
  private val RouteCache = CachingDirectives.routeCache(1000,16,Duration.Inf,Duration("30 min"))
  
  val finder = system.actorOf(Props(new MasterActor("FindMaster")), name="FindMaster")
  val indexer = system.actorOf(Props(new MasterActor("IndexMaster")), name="IndexMaster")

  val monitor = system.actorOf(Props(new MasterActor("StatusMaster")), name="StatusMaster")
  val registrar = system.actorOf(Props(new MasterActor("MetaMaster")), name="MetaMaster")
  
  val tracker = system.actorOf(Props(new MasterActor("TrackMaster")), name="TrackMaster")
  val trainer = system.actorOf(Props(new MasterActor("TrainMaster")), name="TrainMaster")
 
  def start() {
    RestService.start(routes,system,host,port)
  }

  /*
   * The routes defines the different access channels this API supports
   */
  private def routes:Route = {

    path("admin" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
          ctx => doAdmin(ctx,segment)
	    }
	  }
    }  ~ 
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
    path("fields" / Segment) {subject =>   
	  post {
	    respondWithStatus(OK) {
	      ctx => doFields(ctx,subject)
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
    path("get" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,service,subject)
	    }
	  }
    }  ~ 
    /*
     * This request provides trackable information either as an event 
     * or as a feature; an event refers to a certain 'item', e.g. an 
     * ecommerce product or service , and a feature refers to a specific
     * dataset
     */
    path("train" / Segment / Segment) {(service,subject) =>
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,service,subject)
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
   * Common method to handle all admin requests sent to the REST API
   */
  private def doAdmin[T](ctx:RequestContext,task:String) = {

    task match {
      /*
       * Retrieve status of all actors supported 
       */
      case "actors" => {
        
        val names = Seq("FindMaster","IndexMaster","MetaMaster","StatusMaster","TrackMaster","TrainMaster")
        val response = ActorMonitor.isAlive(names)
        
        ctx.complete(response)
        
      }
      
      case _ => ctx.complete("This task is not supported.")

    }
    
  }

  /**
   * 'fields' and 'register' requests refer to the metadata management of
   * Predictiveworks; for a certain task (uid) and a specific model (name), 
   * a specification of the respective data fields can be registered and 
   * retrieved from a Redis database.
   */
  private def doFields[T](ctx:RequestContext,service:String) = doRequest(ctx,service,"fields")
 
  private def doRegister[T](ctx:RequestContext,service:String,subject:String) = {
    
    val task = "register" + ":" + subject
    service match {
      
	  case "association" => {
	    
	    val topics = List("item")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
	  case "context" => {
	    
	    val topics = List("feature")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
      case "decision" => {
	    
	    val topics = List("feature")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
      }      
      
      case "intent" => {
	    
	    val topics = List("amount")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
	  case "outlier" => {
	    
	    val topics = List("feature","product")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
	  case "series" => {
	    
	    val topics = List("item")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
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
   * 'index' & 'track' requests support data registration in an Elasticsearch index
   */   
  private def doIndex[T](ctx:RequestContext,service:String,subject:String) = {

    val task = "index" + ":" + subject   
    service match {

 	  case "association" => {
	    
	    val topics = List("item","rule")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
	  case "context" => {
	    
	    val topics = List("feature")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
      case "decision" => {
	    
	    val topics = List("feature")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
        
      }	      
      case "intent" => {
	    
	    val topics = List("amount")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
      case "outlier" => {
	    
	    val topics = List("feature","product")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
	  case "series" => {
	    
	    val topics = List("item","rule")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	      
	  
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

	  case "association" => {
	    
	    val topics = List("item")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	

	  }
	  case "context" => {
	    
	    val topics = List("feature")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
      case "decision" => {
	    
	    val topics = List("item")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	

      }      
      case "intent" => {
	    
        val topics = List("amount")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
     
      }
	  case "outlier" => {
	    
	    val topics = List("feature","product")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
	  case "series" => {
	    
	    val topics = List("item")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }	
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

  private def doGet[T](ctx:RequestContext,service:String,subject:String) = {

    val task = "get" +":" + subject
    service match {

	  case "association" => {
	    
	    val topics = List("antecedent","consequent","transaction","rule")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	

	  }
	  case "context" => {
	    
	    subject match {	      
	      
	      /* ../get/context/feature */
	      case "feature" => doRequest(ctx,service,"predict:feature")
	      /* ../get/context/similar */
	      case "similar" => doRequest(ctx,service,"similar:feature")
	      
	      case _ => {}
	      
	    }
	    
	  }
      case "decision" => {
	    
        val topics = List("feature")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
      case "intent" => {
	    
        val topics = List("loyalty","purchase")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
	  case "outlier" => {
	    
	    val topics = List("feature","product")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
	  case "series" => {
	    
	    val topics = List("antecedent","consequent","pattern","rule")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
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
	      /* ../get/text/concept */
	      case "concept" => doRequest(ctx,"text","get:concept")	
	      
	      case _ => {}
	      
	    }
	    
	  }

	  case _ => {}
	  
    }
    
  }
  
  private def doTrain[T](ctx:RequestContext,service:String,subject:String) = {

    val task = "train" +":" + subject
    service match {

	  case "association" => {
	    
	    val topics = List("model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	

	  }
	  case "context" => {
	    
	    val topics = List("matrix","model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
      case "decision" => {
	    
        val topics = List("model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
      case "intent" => {
	    
        val topics = List("model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
      
      }
	  case "outlier" => {
	    
	    val topics = List("model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
	  case "series" => {
	    
	    val topics = List("model")
	    if (topics.contains(subject)) doRequest(ctx,service,task)	
	    
	  }
	  case "similarity" => {
	    
	    val topics = List("model")
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
	      /* ../get/text/concept */
	      case "concept" => doRequest(ctx,"text","train:concept")	
	      
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
      
      case "get"   => finder      
      case "index"   => indexer
      
      case "status" => monitor
      case "train" => trainer

      case "register" => registrar
      case "track" => tracker
      
      case _ => null
      
    }
  }
}