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

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.index.IndexResponse

object ListenerUtils {

  def onIndex(responseListener:OnIndexResponseListener[IndexResponse],failureListener:OnFailureListener):ActionListener[IndexResponse] = {
       
    return new ActionListener[IndexResponse]() {

      override def onResponse(response:IndexResponse) {
        responseListener.onResponse(response)
      }

      override def onFailure(e:Throwable) {
        failureListener.onFailure(e)
      }
      
    }
    
  }
  
  def onSearch(responseListener:OnSearchResponseListener[SearchResponse], failureListener:OnFailureListener):ActionListener[SearchResponse] = {
       
    return new ActionListener[SearchResponse]() {

      override def onResponse(response:SearchResponse) {
        responseListener.onResponse(response)
      }

      override def onFailure(e:Throwable) {
        failureListener.onFailure(e)
      }
      
    }
    
  }
  
  trait OnIndexResponseListener[IndexResponse] {
    def onResponse(response:IndexResponse)
  }

  trait OnSearchResponseListener[SearchResponse] {
    def onResponse(response:SearchResponse)
  }

  trait OnFailureListener {
    def onFailure(t:Throwable)
  }

}