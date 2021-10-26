package com.juzishuke.flink.entity

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.nio.charset.StandardCharsets

/**
 * <p>
 *
 * @author Guan Peixiang (guanpeixiang@juzishuke.com)
 * @date 2021/04/14
 */
class DataSchema  extends DeserializationSchema[JSONObject] with SerializationSchema[JSONObject] {
  override def deserialize(message: Array[Byte]): JSONObject = {
    JSON.parseObject(new String(message))
  }

  override def isEndOfStream(nextElement: JSONObject): Boolean = false

  override def serialize(element: JSONObject): Array[Byte] = {
    element.toJSONString.getBytes(StandardCharsets.UTF_8);
  }


  override def getProducedType: TypeInformation[JSONObject] =  {
    TypeInformation.of(classOf[JSONObject])
  }
}