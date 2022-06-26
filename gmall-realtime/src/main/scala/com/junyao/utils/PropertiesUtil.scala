package com.junyao.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author wjy
 * @create 2022-06-24 12:34
 */
object PropertiesUtil {
  //读取配置文件
  def load(propertiesName:String) = {
    val props = new Properties()
    props.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream("propertiesName"),"UTF-8"))
    props
  }
}
