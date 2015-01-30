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

class ElasticPointBuilder {

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
 
                /* row */
                .startObject("row")
                  .field("type", "long")
                  .field("index", "not_analyzed")
                .endObject()
                    
                /* col */
                .startObject("col")
                   .field("type", "long")
                   .field("index", "not_analyzed")
                .endObject()
                
                /* cat */
                .startObject("cat")
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()

                /* val */
                .startObject("val")
                   .field("type", "double")
                   .field("index", "not_analyzed")
                .endObject()

              .endObject() // properties
            .endObject()   // mapping
          .endObject()
                    
    builder

  }
  
  def createSourceJSON(params:Map[String,String]):XContentBuilder = {
    
    val uid = params("uid")
    
    val row = params("row").toLong
    val col = params("col").toLong
    
    val cat = params("cat")
    val value = params("val")

    val builder = XContentFactory.jsonBuilder()
	builder.startObject()

	builder.field("uid", uid)
	  
	builder.field("row", row)
    builder.field("col", col)

    builder.field("cat", cat)
    builder.field("val", value)
	  
    builder.endObject()
    builder
    
  }
}