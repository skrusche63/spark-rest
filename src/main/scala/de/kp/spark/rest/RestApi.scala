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

import de.kp.spark.rest.actor.{FindMaster,InsightMaster,MetaMaster,StatusMaster,TrackMaster,TrainMaster}
import de.kp.spark.rest.cache.ActorMonitor

import de.kp.spark.rest.model._

class RestApi(host:String,port:Int,system:ActorSystem) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  val (heartbeat,time) = Configuration.actor      
  private val RouteCache = CachingDirectives.routeCache(1000,16,Duration.Inf,Duration("30 min"))
  
//  /* 
//   * The master actor that handles all insight requests 
//   */
//  val insightMaster = system.actorOf(Props[InsightMaster], name="insight-master")
//  
  val finder = system.actorOf(Props[FindMaster], name="FindMaster")

  val monitor = system.actorOf(Props[StatusMaster], name="StatusMaster")
  val registrar = system.actorOf(Props[MetaMaster], name="MetaMaster")
  
  val tracker = system.actorOf(Props[TrackMaster], name="TrackMaster")
  val trainer = system.actorOf(Props[TrainMaster], name="TrainMaster")
 
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
    path("get" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,service,subject)
	    }
	  }
    }  ~ 
    /*
     * This request provides a metadata specification that has to be
     * registered in a Redis instance by the 'meta' service
     */
    path("metadata" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doMetadata(ctx,segment)
	    }
	  }
    }  ~ 
    path("query") {
	  post {
	    respondWithStatus(OK) {
	      ctx => doQuery(ctx)
	    }
	  }
    }  ~ 
    path("status" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,segment)
	    }
	  }
    }  ~ 
    /*
     * This request provides trackable information either as an event 
     * or as a feature; an event refers to a certain 'item', e.g. an 
     * ecommerce product or service , and a feature refers to a specific
     * dataset
     */
    path("track" / Segment) {segment => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrack(ctx, segment)
	    }
	  }
    }  ~ 
    path("train" / Segment) {segment =>
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,segment)
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
        
        val names = Seq("FindMaster","MetaMaster","StatusMaster","TrackMaster","TrainMaster")
        val response = ActorMonitor.isAlive(names)
        
        ctx.complete(response)
        
      }
      
      case _ => ctx.complete("This task is not supported.")

    }
    
  }
  
  private def doMetadata[T](ctx:RequestContext,service:String) = doMetaRequest(ctx,service)

  private def doQuery[T](ctx:RequestContext,service:String="insight") = {
     
    val request = new InsightRequest(service,getRequest(ctx))      
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = "Query is not implemented yet."
    ctx.complete(response)
   
  }

  private def doTrack[T](ctx:RequestContext,topic:String) = {
    
    val request = new TrackRequest(topic,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(tracker,request).mapTo[TrackResponse] 
    ctx.complete(response)
    
  }

  private def doGet[T](ctx:RequestContext,service:String,subject:String) = {

    service match {

	  case "association" => {
	    
	    subject match {	      
	      /* ../get/association/followers */
	      case "followers" => doRequest(ctx,"association","get:followers")	      
	      /* ../get/association/items */
	      case "items" => doRequest(ctx,"association","get:items")
	      /* ../get/association/rules */
	      case "rules" => doRequest(ctx,"rule","get:rules")
	      
	      case _ => {}
	      
	    }

	  }
	  case "context" => {
	    
	    subject match {	      
	      /* ../get/context/prediction */
	      case "prediction" => doRequest(ctx,"context","get:prediction")
	      
	      case _ => {}
	      
	    }
	    
	  }
      case "decision" => {
	    
	    subject match {	      
	      /* ../get/decision/prediction */
	      case "prediction" => doRequest(ctx,"decision","get:prediction")
	      
	      case _ => {}
	      
	    }
      
      }
      case "intent" => {
	    
	    subject match {	      
	      /* ../get/intent/prediction */
	      case "prediction" => doRequest(ctx,"intent","get:prediction")
	      
	      case _ => {}
	      
	    }
      
      }
	  case "outlier" => {
	    
	    subject match {
	      /* ../get/outlier/outliers */
	      case "outliers" => doRequest(ctx,"outlier","get:outliers")
	      
	      case _ => {}
	    
	    }
	    
	  }
	  case "series" => {
	    
	    subject match {
	      /* ../get/series/followers */
	      case "followers" => doRequest(ctx,"series","get:followers")	
	      /* ../get/series/patterns */
	      case "patterns" => doRequest(ctx,"series","get:patterns")
	      /* ../get/series/rules */
	      case "rules" => doRequest(ctx,"series","get:rules")
	      
	      case _ => {}
	      
	    }
	    
	  }
	  case "similarity" => {
	    
	    subject match {
	      /* ../get/similarity/features */
	      case "features" => doRequest(ctx,"similarity","get:features")	
	      /* ../get/similarity/sequences */
	      case "sequences" => doRequest(ctx,"similarity","get:sequences")
	      
	      case _ => {}
	      
	    }
	    
	  }
	  case "social" => {
	    
	    subject match {
	      /* Not implemented yet */
	      case _ => {}
	      
	    }
	    
	  }
	  case "text" => {
	    
	    subject match {
	      /* ../get/text/concepts */
	      case "concepts" => doRequest(ctx,"text","get:concepts")	
	      
	      case _ => {}
	      
	    }
	    
	  }

	  case _ => {}
	  
    }
    
  }
  private def doTrain[T](ctx:RequestContext,service:String) = {

    /* Train requests are distinguished by their targeted services */
    service match {
      
	  case "association" => doRequest(ctx,"association","train")

	  case "context"  => doRequest(ctx,"context","train")
	  case "decision" => doRequest(ctx,"decision","train")

	  case "intent"  => doRequest(ctx,"intent","train")
	  case "outlier" => doRequest(ctx,"outlier","train")

	  case "series"     => doRequest(ctx,"series","train")
	  case "similarity" => doRequest(ctx,"similarity","train")

	  case "social" => doRequest(ctx,"social","train")
	  case "text"   => doRequest(ctx,"text","train")
	  
    }
  
  }

  private def doStatus[T](ctx:RequestContext,service:String) = {

    /* Status requests are distinguished by their targeted services */
    service match {
      
	  case "association" => doRequest(ctx,"association","status")

	  case "context"  => doRequest(ctx,"context","status")
	  case "decision" => doRequest(ctx,"decision","status")

	  case "intent"  => doRequest(ctx,"intent","status")
	  case "outlier" => doRequest(ctx,"outlier","status")

	  case "series"     => doRequest(ctx,"series","status")
	  case "similarity" => doRequest(ctx,"similarity","status")

	  case "social" => doRequest(ctx,"social","status")
	  case "text"   => doRequest(ctx,"text","status")
      
	  case _ => {}
	  
    }
   
  }
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String="train") = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(master(task),request).mapTo[ServiceResponse] 
    ctx.complete(response)
    
  }
  
  private def doMetaRequest[T](ctx:RequestContext,target:String) = {
     
    val request = getBodyAsString(ctx)
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(registrar,request).mapTo[String] 
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
  /**
   * This method returns the 'raw' body provided with a Http request;
   * it is e.g. used to access the meta service to register metadata
   * specifications
   */
  private def getBodyAsString(ctx:RequestContext):String = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    httpEntity.data.asString
    
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
      case "train" => trainer
      
      case "status" => monitor
      
      case _ => null
      
    }
  }
}