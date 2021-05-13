//package com.bbva.ebdm.ocelot.apps.simm_load
//
//import org.apache.spark.sql.{DataFrame}
//import wvlet.log.LogSupport
//import wvlet.airframe._
//import org.apache.log4j.{Level, Logger}
//
//import com.bbva.ebdm.ocelot.engines.spark.SparkService
//import com.bbva.ebdm.ocelot.templates.batch.BatchBaseApp
//
//class SIMMLoadApp extends BatchBaseApp with LogSupport with Utils {
//
//  override def process(inputMap: Map[String, DataFrame]): Map[String, DataFrame] = {
//    info("OCELOT INICIO DEL PROCESS")
//    println("OCELOT INICIO DEL PROCESS")
//    inputMap.foreach{ case(k,v) =>
//      info(s"OCELOT MAP: la clave es $k, y el count es: ${v.count()}")
//      println(s"OCELOT MAP: la clave es $k, y el count es: ${v.count()}")
//    }
//    info("OCELOT FIN DEL PROCESS")
//    println("OCELOT FIN DEL PROCESS")
//    Map()
//  }
//
//}
//
//object SIMMLoadApp {
//  val design = BatchBaseApp.design + SparkService.design
//}
//object Main {
//  def main(args: Array[String]): Unit = {
//    val design = SIMMLoadApp.design + newDesign.bind[Array[String]].toInstance(args)
//    design.build[SIMMLoadApp] { app: SIMMLoadApp => app.exec() }
//  }
//}
