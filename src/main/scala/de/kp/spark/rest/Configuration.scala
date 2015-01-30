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

import com.typesafe.config.ConfigFactory

object Configuration {

    /* Load configuration for router */
  val path = "application.conf"
  val config = ConfigFactory.load(path)

  def actor:(Int,Int,Int) = {
  
    val cfg = config.getConfig("actor")

    val duration = cfg.getInt("duration")
    val retries = cfg.getInt("retries")  
    val timeout = cfg.getInt("timeout")
    
    (duration,retries,timeout)
    
  }

  def admin():(String,Int) = {
      
    val cfg = config.getConfig("admin")
      
    val host = cfg.getString("host")
    val port = cfg.getInt("port")

    (host,port)
    
  }

  def cache():Int = {
  
    val cfg = config.getConfig("cache")
    val size = cfg.getInt("size")
    
    size
    
  }

  def find():(String,Int) = {
      
    val cfg = config.getConfig("find")
      
    val host = cfg.getString("host")
    val port = cfg.getInt("port")

    (host,port)
    
  }
 
  def redis:(String,String) = {
  
    val cfg = config.getConfig("redis")
    
    val host = cfg.getString("host")
    val port = cfg.getString("port")
    
    (host,port)
    
  }

  def router():(Int,Int,Int) = {
  
    val cfg = config.getConfig("router")
  
    val time    = cfg.getInt("time")
    val retries = cfg.getInt("retries")  
    val workers = cfg.getInt("workers")
    
    (time,retries,workers)

  }

  def track():(String,Int) = {
      
    val cfg = config.getConfig("track")
      
    val host = cfg.getString("host")
    val port = cfg.getInt("port")

    (host,port)
    
  }

  def train():(String,Int) = {
      
    val cfg = config.getConfig("train")
      
    val host = cfg.getString("host")
    val port = cfg.getInt("port")

    (host,port)
    
  }

  def web():(String,Int) = {
      
    val cfg = config.getConfig("web")
      
    val host = cfg.getString("host")
    val port = cfg.getInt("port")

    (host,port)
    
  }
  
}