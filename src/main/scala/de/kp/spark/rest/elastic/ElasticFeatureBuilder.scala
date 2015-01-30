package de.kp.spark.rest.elastic
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

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class ElasticFeatureBuilder {
  
  def createBuilder(mapping:String,names:List[String],types:List[String]):XContentBuilder = {
    /*
     * Define mapping schema for index 'index' and 'type'
     */
    val builder = XContentFactory.jsonBuilder()
    builder
      .startObject()
        .startObject(mapping)
          .startObject("properties")

            /* uid */
            .startObject("uid")
              .field("type", "string")
              .field("index", "not_analyzed")
            .endObject()
                    
            /* site */
            .startObject("site")
              .field("type", "string")
              .field("index", "not_analyzed")
            .endObject()

            /* timestamp */
            .startObject("timestamp")
              .field("type", "long")
            .endObject()

    (0 until names.length).foreach( i => {
      builder
        .startObject(names(i))
          .field("type",types(i))
        .endObject()

    })

    builder
          .endObject() // properties
        .endObject()   // mapping
      .endObject()
                    
    builder

  }
 
  def createSource(params:Map[String,String]):XContentBuilder = {
    
    val uid = params("uid")    
    val site = params("site")
    
    val now = new java.util.Date()
    val timestamp = now.getTime()  
 
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()

	builder.field("uid", uid)
	  
	builder.field("site", site)
    builder.field("timestamp", timestamp)

    /* 
     * Restrict parameters to those that are relevant to feature description;
     * note, that we use a flat JSON data structure for simplicity and distinguish
     * field semantics by different prefixes 
     */
    val records = params.filter(kv => kv._1.startsWith("lbl.") || kv._1.startsWith("fea."))
    for (rec <- records) {
      
      val (k,v) = rec
        
      val name = k.replace("lbl.","").replace("fea.","")
      builder.field(name, v)      
      
    }
	  
    builder.endObject()
    builder
     
  }

}