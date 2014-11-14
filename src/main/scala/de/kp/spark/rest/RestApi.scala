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
    path("get" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,service,subject)
	    }
	  }
    }  ~ 
    path("index" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doIndex(ctx,service,subject)
	    }
	  }
    }  ~ 
    /*
     * This request provides a metadata specification that has to be
     * registered in a Redis instance by the 'meta' service
     */
    path("register" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doRegister(ctx,service,subject)
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
    path("track" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrack(ctx, service, subject)
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
        
        val names = Seq("FindMaster","IndexMaster","MetaMaster","StatusMaster","TrackMaster","TrainMaster")
        val response = ActorMonitor.isAlive(names)
        
        ctx.complete(response)
        
      }
      
      case _ => ctx.complete("This task is not supported.")

    }
    
  }
   
  private def doIndex[T](ctx:RequestContext,service:String,subject:String) = {

    val task = "index"    
    service match {

      /* ../index/association/item */
	  case "association" => doRequest(ctx,"association",task)	      
	  /* ../index/context/feature */
	  case "context" => doRequest(ctx,"context",task)	      
	  /* ../index/decision/feature */
      case "decision" => doRequest(ctx,"decision",task)	      
      
      case "intent" => {
	    
	    subject match {	      
	      /* ../index/intent/amount */
	      case "amount" => doRequest(ctx,"intent",task+":amount")
	      
	      case _ => {}
	      
	    }
      
      }
	  case "outlier" => {
	    
	    subject match {
	      /* ../index/outlier/feature */
	      case "feature" => doRequest(ctx,"outlier",task+":feature")
	      /* ../index/outlier/sequence */
	      case "sequence" => doRequest(ctx,"outlier",task+":sequence")
	      
	      case _ => {}
	    
	    }
	    
	  }
	  /* ../index/series/item */
	  case "series" => doRequest(ctx,"series",task)	      
	  
	  case "similarity" => {
	    
	    subject match {
	      /* ../index/similarity/feature */
	      case "feature" => doRequest(ctx,"similarity",task+":feature")	
	      /* ../index/similarity/sequence */
	      case "sequence" => doRequest(ctx,"similarity",task+":sequence")
	      
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
	      /* Not implemented yet */
	      case _ => {}
	      
	    }
	    
	  }

	  case _ => {}
	  
    }
    
  }
 
  private def doRegister[T](ctx:RequestContext,service:String,subject:String) = {
    
    val task = "register"
    service match {
      /* ../register/association/field */
	  case "association" => doRequest(ctx,"association",task)	      
	  /* ../register/context/feature */
	  case "context" => doRequest(ctx,"context",task)	      
	  /* ../register/decision/feature */
      case "decision" => doRequest(ctx,"decision",task)	      
      
      case "intent" => {
	    
	    subject match {	      
	      /* ../register/intent/loyalty */
	      case "loyalty" => doRequest(ctx,"intent",task+":loyalty")

	      /* ../register/intent/purchase */
	      case "purchase" => doRequest(ctx,"intent",task+":purchase")
	      
	      case _ => {}
	      
	    }
      
      }
	  case "outlier" => {
	    
	    subject match {
	      /* ../register/outlier/feature */
	      case "feature" => doRequest(ctx,"outlier",task+":feature")
	      /* ../register/outlier/sequence */
	      case "sequence" => doRequest(ctx,"outlier",task+":sequence")
	      
	      case _ => {}
	    
	    }
	    
	  }
	  /* ../register/series/field */
	  case "series" => doRequest(ctx,"series","register")	      
	  
	  case "similarity" => {
	    
	    subject match {
	      /* ../register/similarity/feature */
	      case "feature" => doRequest(ctx,"similarity",task+":feature")	
	      /* ../register/similarity/sequence */
	      case "sequence" => doRequest(ctx,"similarity",task+":sequence")
	      
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
	      /* Not implemented yet */
	      case _ => {}
	      
	    }
	    
	  }

	  case _ => {}
	  
    }
    
  }

  private def doQuery[T](ctx:RequestContext,service:String="insight") = {
   
    val response = "Query is not implemented yet."
    ctx.complete(response)
   
  }

  /**
   * A track request has the following request url: /track/{service}/{topic}
   * 
   * Topics are from the set: amount, item, feature, sequence
   * 
   */
  private def doTrack[T](ctx:RequestContext,service:String,subject:String) = {

    val task = "track"
    service match {

	  /* ../track/association/item */
	  case "association" => doRequest(ctx,"association",task)	      

      /* ../track/context/feature */
	  case "context" => doRequest(ctx,"context",task)	      
	  
	  /* ../track/decision/feature */
      case "decision" => doRequest(ctx,"decision",task)	      
      
      case "intent" => {
	    
	    subject match {	      
	      /* ../track/intent/amount */
	      case "amount" => doRequest(ctx,"intent",task+":amount")
	      
	      case _ => {}
	      
	    }
      
      }
	  case "outlier" => {
	    
	    subject match {
	      /* ../track/outlier/feature */
	      case "feature" => doRequest(ctx,"outlier",task+":feature")
	      /* ../track/outlier/sequence */
	      case "sequence" => doRequest(ctx,"outlier",task+":sequence")
	      
	      case _ => {}
	    
	    }
	    
	  }
	  /* ../track/series/item */
	  case "series" => doRequest(ctx,"series",task)	
	  
	  case "similarity" => {
	    
	    subject match {
	      /* ../track/similarity/feature */
	      case "feature" => doRequest(ctx,"similarity",task+":feature")	
	      /* ../track/similarity/sequence */
	      case "sequence" => doRequest(ctx,"similarity",task+":sequence")
	      
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
	      /* Not implemented yet */
	      case _ => {}
	      
	    }
	    
	  }

	  case _ => {}
	  
    }
    
  }

  private def doGet[T](ctx:RequestContext,service:String,subject:String) = {

    val task = "get"
    service match {

	  case "association" => {
	    
	    subject match {	      
	      /* ../get/association/antecedent */
	      case "antecedent" => doRequest(ctx,"association",task+":antecedent")	      
	      /* ../get/association/consequent */
	      case "consequent" => doRequest(ctx,"association",task+":consequent")	      
	      /* ../get/association/transaction */
	      case "transaction" => doRequest(ctx,"association",task+":transaction")
	      /* ../get/association/rule */
	      case "rule" => doRequest(ctx,"association",task+":rule")
	      
	      case _ => {}
	      
	    }

	  }
	  case "context" => {
	    
	    subject match {	      
	      /* ../get/context/prediction */
	      case "prediction" => doRequest(ctx,"context",task+":prediction")
	      
	      case _ => {}
	      
	    }
	    
	  }
      case "decision" => {
	    
	    subject match {	      
	      /* ../get/decision/prediction */
	      case "prediction" => doRequest(ctx,"decision",task+":prediction")
	      
	      case _ => {}
	      
	    }
      
      }
      case "intent" => {
	    
	    subject match {	      
	      /* ../get/intent/loyalty */
	      case "loyalty" => doRequest(ctx,"intent",task+":loyalty")

	      /* ../get/intent/purchase */
	      case "purchase" => doRequest(ctx,"intent",task+":purchase")
	      
	      case _ => {}
	      
	    }
      
      }
	  case "outlier" => {
	    
	    subject match {
	      /* ../get/outlier/feature */
	      case "feature" => doRequest(ctx,"outlier",task+":feature")
	      /* ../get/outlier/sequence */
	      case "sequence" => doRequest(ctx,"outlier",task+":sequence")
	      
	      case _ => {}
	    
	    }
	    
	  }
	  case "series" => {
	    
	    subject match {
	      /* ../get/series/antecedent */
	      case "antecedent" => doRequest(ctx,"series",task+":antecedent")	
	      /* ../get/series/consequent */
	      case "consequent" => doRequest(ctx,"series",task+":consequent")	
	      /* ../get/series/pattern */
	      case "pattern" => doRequest(ctx,"series",task+":pattern")
	      /* ../get/series/rule */
	      case "rule" => doRequest(ctx,"series",task+":rule")
	      
	      case _ => {}
	      
	    }
	    
	  }
	  case "similarity" => {
	    
	    subject match {
	      /* ../get/similarity/feature */
	      case "feature" => doRequest(ctx,"similarity",task+":feature")	
	      /* ../get/similarity/sequence */
	      case "sequence" => doRequest(ctx,"similarity",task+":sequence")
	      
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
	      /* ../get/text/concept */
	      case "concept" => doRequest(ctx,"text",task+":concept")	
	      
	      case _ => {}
	      
	    }
	    
	  }

	  case _ => {}
	  
    }
    
  }

  private def doStatus[T](ctx:RequestContext,service:String) = {

    if (Services.isService(service) == true) {
      doRequest(ctx,service,"status")
    }
   
  }
  
  private def doTrain[T](ctx:RequestContext,service:String) = {

    if (Services.isService(service) == true) {
      doRequest(ctx,service,"train")
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