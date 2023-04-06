package cn.pandadb.browser.utils

import cn.pandadb.browser.VO.PandadbConnectionInfo
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNode, LynxPath, LynxRelationship}
import org.grapheco.pandadb.driver.PandaDBDriver

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class PandadbQueryTool(connectInfo: PandadbConnectionInfo) {


  val host = connectInfo.getHost
  val port = connectInfo.getPort

  val client = new PandaDBDriver(host, port)

  def getDataByCql(cypher: String): util.Map[String, Any] = {
    val retMap = new util.HashMap[String, Any]

    val mapDataList = new ListBuffer[util.Map[String, util.Map[String, Any]]]
    val nodesList = new ListBuffer[util.Map[String, Any]]
    val relationsList = new ListBuffer[util.Map[String, Any]]
    val retColumns = new util.HashSet[String]()
    val columns = new util.HashSet[String]()

    val result = client.query(cypher)

    while (result.hasNext) {
      val lineMapData = new util.HashMap[String, util.Map[String, Any]]()
      mapDataList.append(lineMapData)

      val value = result.next()

      for ((k, v) <- value) {
        retColumns.add(k)

        if (v.isInstanceOf[LynxNode]) {

          val nodeMap = lynxNodeDeal(v.asInstanceOf[LynxNode])
          val mapData = new mutable.HashMap[String, Any]()

          nodesList.append(nodeMap.asJava)
          columns.addAll(nodeMap.get("labels").get.asInstanceOf[util.Collection[String]]);

          mapData.put("type", "object")
          mapData.put("data", nodeMap.get("properties").get)
          lineMapData.put(k, mapData.asJava)

        } else if (v.isInstanceOf[LynxList]) {
          val list = v.asInstanceOf[LynxList].value

          val listdata = new ListBuffer[util.Map[String, Any]]
          val mapData = new mutable.HashMap[String, Any]()

          list.foreach(l => {
            if (l.isInstanceOf[LynxRelationship]) {

              val relaMap = lynxRelationshipDeal(l.asInstanceOf[LynxRelationship])
              relationsList.append(relaMap.asJava)

              listdata.append(relaMap.get("properties").get.asInstanceOf[util.Map[String, Any]])
            }
          })

          mapData.put("type", "array")
          mapData.put("data", listdata.asJava)
          lineMapData.put(k, mapData.asJava)

        } else if (v.isInstanceOf[LynxRelationship]) {
          val mapData = new mutable.HashMap[String, Any]()
          val relaMap = lynxRelationshipDeal(v.asInstanceOf[LynxRelationship])
          relationsList.append(relaMap.asJava)

          mapData.put("type", "object")
          mapData.put("data", relaMap.get("properties").get)
          lineMapData.put(k, mapData.asJava)
        } else if (v.isInstanceOf[LynxPath]) {

          val path = v.asInstanceOf[LynxPath]
          val pathData = new ListBuffer[util.Map[String, Any]]

          //val list1 = new ListBuffer[ListBuffer[Any]]
          //var list2 = new ListBuffer[Any]
          var c = 1;

          path.elements.foreach(p => {
            if (c %2 == 1) {
              val node = p.asInstanceOf[LynxNode]
              val map = lynxNodeDeal(node)
              nodesList.append(map.asJava)

              columns.addAll(map.get("labels").get.asInstanceOf[util.Collection[String]]);
              pathData.append(map.get("properties").get.asInstanceOf[util.Map[String, Any]])
            } else if (c % 2 == 0) {
              val rel = p.asInstanceOf[LynxRelationship]
              val map = lynxRelationshipDeal(rel)
              relationsList.append(map.asJava)

              pathData.append(map.get("properties").get.asInstanceOf[util.Map[String, Any]])
            }
            c = c + 1;
          })

          val mapData = new util.HashMap[String, Any]()
          mapData.put("type", "array")
          mapData.put("data", pathData.asJava)
          lineMapData.put(k, mapData)

        } else {
          val mapData = new mutable.HashMap[String, Any]()

          mapData.put("type", "string")
          val data = v match {
            case n: LynxList => seqAsJavaList(n.value.map(f => f.value))
            case n: List[LynxValue] => seqAsJavaList(n.map(f => f.value))
            case n: Map[String, LynxValue] => n.map(f => (f._1, f._2.value)).asJava
            case n: LynxValue => n.value.toString
            case n => n.value.toString
          }
          mapData.put("data", data)

          lineMapData.put(k, mapData.asJava)
        }
      }
    }

    val graph = new mutable.HashMap[String, Any]()
    val tuple = filterListById(nodesList, relationsList)
    graph.put("nodes", tuple._1.asJava)
    graph.put("relationships", tuple._2.asJava)
    //graph.put("relationships", relationsList.asJava)

    val resultMap = new util.HashMap[String, Any]()
    resultMap.put("data", mutable.ListBuffer(mutable.HashMap(("graph", graph.asJava)).asJava).asJava)
    resultMap.put("columns", columns)

    retMap.put("results", mutable.ListBuffer(resultMap).asJava)
    retMap.put("columns", retColumns)
    retMap.put("mapData", mapDataList.asJava)

    retMap
  }

  def lynxRelationshipDeal(relationship: LynxRelationship) = {
    val relaMap = new mutable.HashMap[String, Any]()
    //val mapData = new mutable.HashMap[String, Any]()

    val id = relationship.id.value.asInstanceOf[LynxValue].value
    val startId = relationship.startNodeId.value.asInstanceOf[LynxInteger].value
    val endId = relationship.endNodeId.value.asInstanceOf[LynxInteger].value
    val relationType = relationship.relationType.get.value
    val property = relationship.keys.map(k => (k.value -> relationship.property(k).get.value))


    val _property = property.map(kv => (kv._1, kv._2 match {
      case n: LynxList => seqAsJavaList(n.value.map(f => f.value))
      case n: List[LynxValue] => seqAsJavaList(n.map(f => f.value))
      case n: Map[String, LynxValue] => n.map(f => (f._1, f._2.value)).asJava
      case n: LynxValue => n.value
      case n => n.toString
    }))
    val propertyMap = new mutable.HashMap[String, Any]()
    _property.foreach(f => {
      propertyMap.put(f._1, f._2)
    })

    relaMap.put("id", id + "")
    relaMap.put("startNode", startId + "")
    relaMap.put("endNode", endId + "")
    relaMap.put("type", relationType)
    relaMap.put("properties", propertyMap.asJava)

    relaMap
  }

  def lynxNodeDeal(node: LynxNode) = {
    val nodeMap = new mutable.HashMap[String, Any]()
    //val mapData = new mutable.HashMap[String, Any]()

    val id = node.id.value.asInstanceOf[LynxInteger].value
    val property = node.keys.map(k => (k.value -> node.property(k).get.value))

    val _property = property.map(kv => (kv._1, kv._2 match {
      case n: LynxList => seqAsJavaList(n.value.map(f => f.value))
      case n: List[LynxValue] => seqAsJavaList(n.map(f => f.value))
      case n: Map[String, LynxValue] => n.map(f => (f._1, f._2.value)).asJava
      case n: LynxValue => n.value
      case n => n.toString
    }))


    val propertyMap = new mutable.HashMap[String, Any]()
    _property.foreach(f => {
      propertyMap.put(f._1, f._2)
    })

    val label = node.labels.map(f => f.value)
    nodeMap.put("id", id + "")
    nodeMap.put("labels", label.asJava)
    nodeMap.put("properties", propertyMap.asJava)

    nodeMap

  }

  def filterListById(nodes: ListBuffer[util.Map[String, Any]], relationship: ListBuffer[util.Map[String, Any]]) = {
    val nodeMap = new mutable.HashMap[String, Any]()
    val relationshipMap = new mutable.HashMap[String, Any]()
    if (nodes == null || nodes.size == 0) {

    } else {
      nodes.foreach(node => {
        nodeMap.put(node.get("id").toString, node)
      })
    }
    if (relationship == null || relationship.size == 0) {

    } else {
      relationship.foreach(rel => {
        if (nodeMap.contains(rel.get("startNode").toString) &&
          nodeMap.contains(rel.get("endNode").toString)) {
          relationshipMap.put(rel.get("id").toString, rel);
        }
      })
    }
    (nodeMap.values, relationshipMap.values)
  }

}
