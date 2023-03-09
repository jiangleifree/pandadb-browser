package cn.pandadb.browser.utils

import cn.pandadb.browser.VO.PandadbConnectionInfo

import java.util
import java.util.regex.Pattern
import scala.collection.mutable

/**
 * @program: TestClientWithStatistics
 * @description:
 * @author: LiamGao
 * @create: 2022-05-15 21:39
 */
object PatternProcess {
  val basicNodeLength = "match()returncount()".length
  val basicRelLength = "match()-[]->()returncount()".length


  def getDataByCypher(info: PandadbConnectionInfo): java.util.Map[String, Any] = {

    val retMap = new mutable.HashMap[String, Any]()
    val mapDataList = new mutable.ListBuffer[util.Map[String, Any]]()
    //val aa = checkCypher(info.getCypher)

    //val mode = aa.asInstanceOf[CypherMode]
    //val isCountQuery = mode.isGoCount
    //val schema = mode.schema


    val queryTool = new cn.pandadb.browser.utils.PandadbQueryTool(info)
    val columns = new mutable.ListBuffer[String]
    queryTool.getDataByCql(info.getCypher)
  }


  def checkCypher(str: String): CypherMode = {
    val pstr = str.replace(" ", "").toLowerCase()

    val isRelation: Boolean = pstr.contains("""-[""")
    if (isRelation) {
      val left = pstr.indexOf("[")
      val right = pstr.indexOf("]", left)
      val relStr = pstr.slice(left + 1, right)
      val relAndType = relStr.split(":")
      checkIsRelationCountCypher(str, pstr, relAndType)
    }
    else {
      val left = pstr.indexOf("match(")
      val right = pstr.indexOf(")", left + 6)
      val nodeStr = pstr.slice(left + 6, right)
      val nodeAndLabel = nodeStr.split(":") // length = 1 or 2
      checkIsNodeCountCypher(str, pstr, nodeAndLabel)
    }
  }

  def checkIsRelationCountCypher(originCypher: String, noBlankLowerStr: String, array: Array[String]): CypherMode = {
    val startNodePattern = Pattern.compile("""\(.*\)-""")
    val endNodePattern = Pattern.compile("""->\(.*\) """)
    val start = startNodePattern.matcher(originCypher)
    val end = endNodePattern.matcher(originCypher)
    val f1 = start.find()
    val f2 = end.find()
    if (!f1 || !f2)
      return CypherMode(false, Map.empty)

    val l1 = getRelNodeLength(start.group())
    val l2 = getRelNodeLength(end.group())

    if (noBlankLowerStr.contains("count")) {
      if (array.length == 1) {
        val relSchema = array.head
        if (noBlankLowerStr.length == basicRelLength + relSchema.length * 2 + l1 + l2) {
          val trueNodeSchemaIndex = originCypher.toLowerCase().indexOf(s"$relSchema")
          val schema = originCypher.slice(trueNodeSchemaIndex, trueNodeSchemaIndex + relSchema.length)
          CypherMode(true, Map("isCountNode" -> "no", "relSchema" -> schema))

        }
        else CypherMode(false, Map.empty)
      }
      else {
        val relSchema = array.head
        val relType = array.last
        if (noBlankLowerStr.length == basicRelLength + relSchema.length * 2 + relType.length + 1 + l1 + l2) {
          val oc = originCypher.toLowerCase()
          val trueNodeSchemaIndex = oc.indexOf(s"$relSchema")
          val trueNodeLabelIndex = oc.toLowerCase().indexOf(s"$relType")
          val schema = originCypher.slice(trueNodeSchemaIndex, trueNodeSchemaIndex + relSchema.length)
          val label = originCypher.slice(trueNodeLabelIndex, trueNodeLabelIndex + relType.length)
          CypherMode(true, Map("isCountNode" -> "no", "relSchema" -> schema, "relType" -> label))
        }
        else CypherMode(false, Map.empty)
      }
    }
    else CypherMode(false, Map.empty)
  }

  def checkIsNodeCountCypher(originCypher: String, noBlankLowerStr: String, array: Array[String]): CypherMode = {
    if (noBlankLowerStr.contains("count")) {
      if (array.length == 1) {
        val nodeSchema = array.head
        if (noBlankLowerStr.length == basicNodeLength + nodeSchema.length * 2) {
          val trueNodeSchemaIndex = originCypher.toLowerCase().indexOf(s"$nodeSchema")
          val schema = originCypher.slice(trueNodeSchemaIndex, trueNodeSchemaIndex + nodeSchema.length)
          CypherMode(true, Map("isCountNode" -> "yes", "nodeSchema" -> schema))
        }
        else CypherMode(false, Map.empty)
      }
      else {
        val nodeSchema = array.head
        val nodeLabel = array.last
        if (noBlankLowerStr.length == basicNodeLength + nodeSchema.length * 2 + nodeLabel.length + 1) {
          val oc = originCypher.toLowerCase()
          val trueNodeSchemaIndex = oc.indexOf(s"$nodeSchema")
          val trueNodeLabelIndex = oc.toLowerCase().indexOf(s"$nodeLabel")
          val schema = originCypher.slice(trueNodeSchemaIndex, trueNodeSchemaIndex + nodeSchema.length)
          val label = originCypher.slice(trueNodeLabelIndex, trueNodeLabelIndex + nodeLabel.length)
          CypherMode(true, Map("isCountNode" -> "yes", "nodeSchema" -> schema, "nodeLabel" -> label))
        }
        else CypherMode(false, Map.empty)
      }
    }
    else CypherMode(false, Map.empty)
  }

  def getRelNodeLength(str: String): Int = {
    val left = str.indexOf("(")
    val right = str.indexOf(")")
    str.slice(left + 1, right).replace(" ", "").length
  }
}

case class CypherMode(isGoCount: Boolean, schema: Map[String, String]) {}
