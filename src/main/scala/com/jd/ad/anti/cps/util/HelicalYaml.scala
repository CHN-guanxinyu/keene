package com.jd.ad.anti.cps.util

import org.yaml.snakeyaml.{Yaml, DumperOptions}
import org.yaml.snakeyaml.representer.{Represent, Representer}
import org.yaml.snakeyaml.nodes.{Node, Tag}
import scala.collection.JavaConversions._

class HelicalYaml  {
	val yaml = new Yaml(new ScalaRepresenter(), new DumperOptions())
	
	def dump(data: Any): String = {
		yaml.dump(data)
	}
	
	def load(data: String): Any = {
		yaml.load(data)
	}

}

class ScalaRepresenter extends Representer {

  multiRepresenters.put(classOf[List[Object]], new RepresentList())
  multiRepresenters.put(classOf[Map[_ <: Object, _ <: Object]], new RepresentMap())

  private class RepresentList extends Represent {
    def representData(data:Object):Node = {
      val scalaList = data.asInstanceOf[List[_ <: Object]]
      return representSequence(getTag(scalaList.getClass(), Tag.SEQ), scalaList, null)
    }
  }
  
  private class RepresentMap extends Represent {
    def representData(data:Object):Node = {
      val scalaMap = data.asInstanceOf[Map[_ <: Object, _ <: Object]]
      return representMapping(getTag(scalaMap.getClass(), Tag.MAP), scalaMap, null)
    }
  }
} 
