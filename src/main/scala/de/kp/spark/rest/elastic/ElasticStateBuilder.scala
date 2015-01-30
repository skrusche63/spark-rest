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

class ElasticStateBuilder {
  
  def createBuilder(mapping:String):XContentBuilder = {

    val builder = XContentFactory.jsonBuilder()
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

                /* user */
                .startObject("user")
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()
 
                /* timestamp */
                .startObject("timestamp")
                  .field("type", "long")
                .endObject()

                /* state */
                .startObject("state")
                   .field("type", "string")
                .endObject()

              .endObject() // properties
            .endObject()   // mapping
          .endObject()
                    
    builder

  }
   def createSourceJSON(params:Map[String,String]):XContentBuilder = {
    
    val uid = params("uid")
    
    val site = params("site")
    val user = params("user")
    
    val timestamp = params("timestamp").toLong
    val state = params("state")

    val builder = XContentFactory.jsonBuilder()
	builder.startObject()

	builder.field("uid", uid)
	  
	builder.field("site", site)
    builder.field("user", user)

    builder.field("timestamp", timestamp)
    builder.field("state", state)
	  
    builder.endObject()
    builder
    
  }

}