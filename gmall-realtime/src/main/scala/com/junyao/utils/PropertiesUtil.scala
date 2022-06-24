package com.junyao.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author wjy
 * @create 2022-06-20 22:37
 */
object PropertiesUtil {

  def load(Properties:String)= {
    val prop = new Properties
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(Properties),"UTF-8"))
    prop
  }

}
