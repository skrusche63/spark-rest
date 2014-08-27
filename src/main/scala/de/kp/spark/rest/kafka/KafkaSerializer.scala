package de.kp.spark.rest.kafka
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

import kafka.serializer.{Decoder, Encoder}
import kafka.utils.VerifiableProperties

import org.apache.commons.io.Charsets

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import de.kp.spark.rest.EventMessage

class EventDecoder(props: VerifiableProperties) extends Decoder[EventMessage] {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def fromBytes(bytes: Array[Byte]): EventMessage = {
    read[EventMessage](new String(bytes, Charsets.UTF_8))
  }

}

class EventEncoder(props: VerifiableProperties) extends Encoder[EventMessage] {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def toBytes(message: EventMessage): Array[Byte] = {
    write[EventMessage](message).getBytes(Charsets.UTF_8)
  }
  
}