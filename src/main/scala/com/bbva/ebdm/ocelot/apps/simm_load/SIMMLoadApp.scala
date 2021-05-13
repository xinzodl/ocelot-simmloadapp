package com.bbva.ebdm.ocelot.apps.simm_load

import scala.util.Try
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.Window
import wvlet.log.LogSupport
import wvlet.airframe._

import com.bbva.ebdm.ocelot.core._
import com.bbva.ebdm.ocelot.engines.spark.SparkService
import com.bbva.ebdm.ocelot.io.hive._
import com.bbva.ebdm.ocelot.templates.batch.BatchBaseApp

class SIMMLoadApp extends BatchBaseApp with LogSupport with Utils {

  case class SIMMLoadCompletion(
    newCommonFilesDF: DataFrame,
    completedLoads: Map[String, DataFrame],
    areAllLoadsComplete: Boolean,
    outputGroups: Seq[Int])

//  // Schemas Read
//  private val MdGsrRead = config.at("MdGsrRead".path[String]).getOrFail("Could not load MdGsrRead from config")
//  private val tmo = MdGsrRead
////  private val tmo = "ud_xe81235"
//  private val MdTraderepo = config.at("MdTraderepo".path[String]).getOrFail("Could not load MdTraderepo from config")
//  private val RdAlgorithmics = config.at("RdAlgorithmics".path[String]).getOrFail("Could not load RdAlgorithmics from config")
//  private val RdRdr = config.at("RdRdr".path[String]).getOrFail("Could not load RdRdr from config")
//  private val RdMentor = config.at("RdMentor".path[String]).getOrFail("Could not load RdMentor from config")
//  private val RdCalypso = config.at("RdCalypso".path[String]).getOrFail("Could not load RdCalypso from config")
//  private val RdEbdmauRead = config.at("RdEbdmauRead".path[String]).getOrFail("Could not load RdEbdmauRead from config")
//
//  // Schemas Write
//  private val RdEbdmauWrite = config.at("RdEbdmauWrite".path[String]).getOrFail("Could not load RdEbdmauWrite from config")
//  private val MdGsrWrite = config.at("MdGsrWrite".path[String]).getOrFail("Could not load MdGsrWrite from config")
//
  // Config Params
  private val appId = config.at("appId".path[String]).getOrFail("Could not load appId from config")
//
//  // "[/de]/proc/ebdmgv/linx/projects/simm/conf/simm.yml" --> SIMMLoadConfig (las masks)
//
//  //  val cfgFilePath = "/proc/ebdmgv/linx/projects" + "/pj01/conf/simm.yml" // No se usa?
//
//  //  private val filterDateFilePath = "/cfg/gsr/initial-margin/fecha_filtro.yml"
//  //  private val filterDate = getFilterDateFromHDFS(filterDateFilePath)
  private val filterDate = config.at("filterDate".path[String]).getOrFail("Could not load config path filterDate")

  //  private val generationRequestFilesPath = "/output/gesces/generationRequests/ready/" // No se usa?

  private val noOutputGenerated: Map[String, DataFrame] = Map()

//  override val in:  Map[String, HiveInput] = Map(
//    "repositoryTradeOperacionesDF" -> HiveInput(MdTraderepo,"treopem_tradeoperaciones_foto"),
//    "repositoryPortfTridaDF" -> HiveInput(RdAlgorithmics,"tgsrr_algorithmics_portftrida"),
//    "repositoryMasterTriresolveDF" -> HiveInput(MdGsrRead,"tgsrm_triresolve"),
//    "repositoryMasterVolfxDF" -> HiveInput(MdGsrRead,"tgsrm_volfx"),
//    "repositoryMasterVolcfDF" -> HiveInput(MdGsrRead,"tgsrm_volcf"),
//    "repositoryMastersimmloadappVolswDF" -> HiveInput(tmo,"tgsrm_volsw", extraFilters = Some(col("year") === 2019 && col("month") === 4)),
////    "repositoryMasterVolswDF" -> HiveInput(MdGsrRead,"tgsrm_volsw"),
//    "repositoryDeltaIrDF" -> HiveInput(tmo,"tgsrm_risk_deltair", extraFilters = Some(col("year") === 2019 && col("month") === 4)),
////    "repositoryDeltaIrDF" -> HiveInput(MdGsrRead,"tgsrm_risk_deltair"),
//    "repositoryCredDF" -> HiveInput(tmo,"tgsrm_risk_cred", extraFilters = Some(col("year") === 2019 && col("month") === 4)),
////    "repositoryCredDF" -> HiveInput(MdGsrRead,"tgsrm_risk_cred"),
//    "repositoryDeltaFxDF" -> HiveInput(MdGsrRead,"tgsrm_risk_fx"),
//    "repositoryVegaIrDF" -> HiveInput(MdGsrRead,"tgsrm_risk_vegair"),
//    "repositoryPlDF" -> HiveInput(MdGsrRead,"tgsrm_risk_pl"),
//    "repositoryContrapartidaDF" -> HiveInput(RdRdr,"tmifidr_rdr_contrapartida_xml", extraFilters = Some(col("last_version") === 1)),
//    "repositoryBucketSimmDF" -> HiveInput(MdGsrRead,"tgsrm_bucket_simm"),
//    "repositoryProductClassSimmDF" -> HiveInput(RdMentor,"tgsrr_mentor_products", extraFilters = Some(col("last_version") === 1)),
//    "calypsoContrapartidasSimmDF" -> HiveInput(RdCalypso,"tgsrr_calypso_counterparty", extraFilters = Some(col("last_version") === 1)),
//    "fileLoadServiceDF" -> HiveInput(RdEbdmauRead,"tauditr_file_load", Some(List("des_path", "des_app_id", "fec_raw_load_date", "des_status", "fec_created_date", "fec_process")), None, Some(col("des_status") === "Processed")))
//
//  override val out: Map[String, HiveOutput] =  Map(
//    "uno" -> HiveOutput(RdEbdmauWrite, "tauditr_file_load", appId = appId, writeMode = SaveMode.Append, repartition = Some(DefaulPartitions)),
//    "dos" -> HiveOutput(MdGsrWrite, "tgsrm_simm", overridePartitions = true, repartition = Some(8), appId = appId)
//  )

  /*
    Se ejecuta con parametro HORA o bien FECHA + HORA
   */
  override def process(inputMap: Map[String, DataFrame]): Map[String, DataFrame] = {
    val testLoad = config.at("isTestLoad".path[Boolean]).getOrFail("Could not find execution setting")
    lazy val appExecutionParams: (Option[String], Option[String]) = (
      config.at("simmLoadDateTime".path[String]).toOption, // [DATE]
      config.at("simmLoadTime".path[String]).toOption) // [TIME]

    if (testLoad) { // sólo carga, para pruebas de carga
      val planDate = config.at("planDate".path[String]).getOrFail("Could not load config path planDate")
      val date = parseDate(planDate, "yyyyMMdd")
      info(s"Ejecutando aplicación SIMMLoadApp sin parámetros. Sólo carga de datos para la fecha: $date")
//      info(s"Finalizada la carga de datos de la aplicación SIMMLoadApp para la fecha: $date")
      loadSIMM(date, inputMap)
    } else { // flujo completo con control de llegada de archivos y generación de salidas
      val (processDate, periodChangeTime) = appExecutionParams match {
        case (None, Some(hour)) =>
          debug(s"Ejecutando aplicación SIMMLoadApp con parámetro hora: $hour")
          // El argumento es la hora, con formato HHmmss
          (new Date(), hour)
        case (Some(time), Some(hour)) =>
          debug(s"Ejecutando aplicación SIMMLoadApp con parámetro hora: $hour y fecha: $time")
          // El primer argumento es la hora y el segundo la fecha,
          // con formatos HHmmss y yyyyMMdd_HHmmss respectivamente
          (parseDate(time, "yyyyMMdd_HHmmss"), hour)
        case _ => {
          error("App execution params error")
          (new Date(), "000000")
        }
      }
      info(s"Ejecutando aplicación SIMMLoadApp con generación de salidas para la fecha: $processDate" +
        s", con hora de cambio de periodo: $periodChangeTime")
      val (periodStartDate, periodEndDate) = getWorkingPeriod(processDate, periodChangeTime)
      debug(s"Periodo laboral calculado: $periodStartDate - $periodEndDate")

      loadSIMM(periodStartDate, periodEndDate, inputMap)
    }
  }

  // --------------------------------------- EVERYTHING ----------------------------------------

  /**
    * Se define una UDF para transformar el campo fec_tradedate desde el formato dd/MM/yyyy al formato yyyyMMdd
    * para poder comparar con la fecha de filtro.
    */
  def dateConversion: String => String = { s => dateHorizonDateToString(s) }
  val dateConversionUDF: UserDefinedFunction = udf(dateConversion)

  def europeanDateToAmericanDate: String => String = { s => spanishDateToAmericanDate(s) }
  val europeanDateToAmericanDateUDF: UserDefinedFunction = udf(europeanDateToAmericanDate)

  /**
    * Realiza todas las cargas para la fecha indicada
    * Utilizado para cargas manuales
    *
    * @param date - fecha del dato
    */
  def loadSIMM(date: Date, params: Map[String, Dataset[Row]]): Map[String, DataFrame]  = {

    // Cargamos y realizamos los filtros correspondientes en ficheros de contrapartidas
    val (contrapartidasInColumsDF, tradeOperacionesDF, plDF) = selectPerimeterCounterparty(date, params)
    //    val perimetroFiltroFechaDF = dateFilter(contrapartidasInColumsDF, contratoInColumsDF)

    // TODO add new fields from pnlVar
    // Lanzar las cargas de las sensibilidades correspondientes
    val deltairDF = loadDeltairAsSIMM(date, contrapartidasInColumsDF, tradeOperacionesDF, plDF, params)
    val creditoDF = loadCreditoAsSIMM(date, contrapartidasInColumsDF, tradeOperacionesDF, plDF, params)
    val fxDF = loadFxAsSIMM(date, contrapartidasInColumsDF, tradeOperacionesDF, plDF, params)
    val vegairDF = loadVegaIrAsSIMM(date, contrapartidasInColumsDF, tradeOperacionesDF, plDF, params)

    debug(s"Unimos los dataframes de todas las sensibilidades")
    // Unir DF de sensibilidades
    val unifiedSIMMDF = deltairDF
      .union(creditoDF)
      .union(fxDF)
      .union(vegairDF)

    //    unifiedSIMMDF.explain()

    // Procesar DF SIMM unificado, aplicando filtros y transformaciones comunes
    processUnifiedSIMMDF(unifiedSIMMDF, date, params)

  }

  /**
    * Comprueba si debe cargar alguna de las sensibilidades a la tabla SIMM,
    * según los archivos cargados en Master
    * Registra los nuevos archivos procesados
    * Comprueba si debe generar salidas
    *
    * @param periodStartDate - fecha de inicio del periodo
    * @param periodEndDate   - fecha de fin del periodo
    */
  def loadSIMM(periodStartDate: Date, periodEndDate: Date, params: Map[String, Dataset[Row]]): Map[String, DataFrame]  = {
    // Comprobar si se han recibido todos los archivos de alguna sensibilidad
    val outputsToSave: Map[String, DataFrame] = checkLoadsFiles(periodStartDate, periodEndDate, params) match {
      case Some(simmLoadCompletion) => {
info("INFO_OCELOTE 1")
simmLoadCompletion.newCommonFilesDF.printSchema()
        val hasNewCommonFiles = !isEmptyDF(simmLoadCompletion.newCommonFilesDF)
        debug(s"Nuevos archivos comunes: $hasNewCommonFiles")
        // Cargamos los ficheros de rdr y calypso para su filtros correspondientes
        // Cargamos y realizamos los filtros correspondientes en ficheros de contrapartidas
        val (contrapartidasInColumsDF, tradeOperacionesDF, plDF) = selectPerimeterCounterparty(periodStartDate, params)

        // Añadimos la fecha de filtro para cada una de las contrapartidas
        //        val perimetroFiltroFechaDF = dateFilter(contrapartidasInColumsDF, contratoInColumsDF)

        // Lanzar las cargas de las sensibilidades correspondientes
        val initialDataFrames = (None, simmLoadCompletion.newCommonFilesDF)
        val (optSIMMDF, newFilesDF) = simmLoadCompletion.completedLoads.foldLeft[(Option[DataFrame], DataFrame)](initialDataFrames) {
          (simmDataFrames, loadData) =>
info("INFO_OCELOTE 2")
loadData._2.printSchema()
            val (optSIMMDF, newFilesDF) = simmDataFrames
            val (loadName, loadNewFilesDF) = loadData
            debug(s"Comprobando carga de: $loadName")
            val optNewSIMMDF = if (hasNewCommonFiles || !isEmptyDF(loadNewFilesDF)) {
              info(s"Lanzando la carga de: $loadName")
              // Si hay archivos nuevos de los comunes, o de la sensibilidad, se lanza la carga
              val loadSIMMDF = loadSensAsSIMM(loadName, periodStartDate, contrapartidasInColumsDF, tradeOperacionesDF, plDF, params)
              optSIMMDF match {
                case None =>
                  debug("Primera carga")
info("INFO_OCELOTE 3")
loadSIMMDF.printSchema()
                  Some(loadSIMMDF)
                case Some(simmDF) =>
                  debug("Otra carga")
info("INFO_OCELOTE 4")
simmDF.printSchema()
                  Some(simmDF.union(loadSIMMDF))
              }
            } else {
              debug("Sin archivos nuevos")
              // Si no hay archivos nuevos, no se debe volver a cargar esta sensibilidad
info("INFO_OCELOTE 5")
Try{
  optSIMMDF.get.printSchema()
}
              optSIMMDF
            }
            // Compone el DF con todos los nuevos archivos recibidos
            val unitedNewFilesDF = newFilesDF.union(loadNewFilesDF)

            (optNewSIMMDF, unitedNewFilesDF)
        }
info("INFO_OCELOTE 6")
newFilesDF.printSchema()

        val outputs: Map[String, DataFrame] = optSIMMDF match {
          case Some(simmDF) =>
            debug("Hay datos nuevos para cargar en SIMM")
            // Procesar DF SIMM unificado, aplicando filtros y transformaciones comunes
            val outputProcessUnifiedSIMMDF = processUnifiedSIMMDF(simmDF, periodStartDate, params)
            debug(s"areAllLoadsComplete ${simmLoadCompletion.areAllLoadsComplete}")
            val outputRequestGeneration: Map[String, DataFrame] = if (simmLoadCompletion.areAllLoadsComplete) {
              //              val outputGroups = simmLoadCompletion.outputGroups
              //              info(s"Se completaron todas las cargas. Generando salidas: $outputGroups")
              //              outputRequestGenerationService.generateGroupRequests(outputGroups, periodStartDate)
              //              info(s"Se completaron todas las cargas. Generando salidas: $outputGroups")
              //              generateGroupRequestsNEW(outputGroups, periodStartDate, generationRequestFilesPath) // TODO tipo de escritura disitna a hive, tratar con ella
              noOutputGenerated
            } else {
              noOutputGenerated
            }
            val outputFileLoadsDFForApp = Map("uno" -> saveFileLoadsDFForApp(newFilesDF, appId))

            // Output Seq
            outputRequestGeneration ++ outputFileLoadsDFForApp ++ outputProcessUnifiedSIMMDF
//            fileLoadService.saveFilesForApp(newFilesDF, appId)
          case None =>{
            // No se ha completado ninguna carga, no se hace nada
            debug("No se ha completado ninguna carga de sensibilidad")
            noOutputGenerated
          }
        }
outputs.foreach{
  info("INFO_OCELOTE 7n")
  _._2.printSchema()
}
        outputs
      }
      case None =>
        // No se han recibido todos los archivos comunes, no se hace nada
        debug("No se han recibido todos los archivos comunes")
        noOutputGenerated
    }
outputsToSave.foreach{
  info("INFO_OCELOTE 8n")
  _._2.printSchema()
}
    outputsToSave
  }

    /**
      * Comprueba qué cargas hay que realizar, y si se debe generar salidas.
      * Además, registra los nuevos archivos encontrados como 'Pending'
      *
      * @param periodStartDate - fecha de inicio del periodo laboral
      * @param periodEndDate - fecha de fin del periodo laboral
      * @return - opcional, objeto que indica qué cargas se han completado
      */
    private def checkLoadsFiles(periodStartDate: Date, periodEndDate: Date, params: Map[String, Dataset[Row]]): Option[SIMMLoadCompletion] = { // TODO OUTPUTS OK
      val startDateStr = formatDate(periodStartDate, "yyyyMMdd")
      val endDateStr = formatDate(periodEndDate, "yyyyMMdd")

      // Obtener los archivos recibidos en el periodo laboral
      val allProcessedFilesDF = params("fileLoadServiceDF").where(
        col("fec_raw_load_date").between(formatDate(periodStartDate, timeFormat), formatDate(periodEndDate, timeFormat)))
//        col("fec_raw_load_date").between(periodStartDate.formatted(timeFormat), periodEndDate.formatted(timeFormat)))
info("INFO_OCELOT: loaded table fileLoadServiceDF")
allProcessedFilesDF.printSchema()
      // Obtener las máscaras de los archivos esperados
      val allSIMMFilesMasks: Seq[String] = config.at("loadMasks".path[Map[String, Seq[String]]]).getOrFail("Could not load config path getAllFilesMasks").values.flatten.toSeq
      // Sustituir las funciones de fecha por las fechas correspondientes
      val allSIMMFilesDatedMasks = replaceDates(allSIMMFilesMasks, startDateStr, endDateStr)
      // Obtener los archivos relacionados con el SIMM
      val simmProcessedFilesDF = filterByFieldMask(allProcessedFilesDF, allSIMMFilesDatedMasks)
      simmProcessedFilesDF.persist

      // Comprobar si se han procesado los archivos comunes
      val commonLoadMasks: Seq[String] = config.at("loadMasks".path[Map[String, Seq[String]]]).getOrFail("Could not load config path getCommonLoadFilesMasks")("common")
      val commonLoadDatedMasks = replaceDates(commonLoadMasks, startDateStr, endDateStr)
      // Obtener únicamente los archivos comunes procesados
      val commonProcessedFilesDF = filterByFieldMask(simmProcessedFilesDF, commonLoadDatedMasks)
      commonProcessedFilesDF.persist
      if (everyMaskExists(commonProcessedFilesDF, commonLoadDatedMasks)) {
        // Se han procesado todos los archivos comunes
        // Comprobar cuáles son posteriores a la última ejecución de la aplicación
        val newCommonFilesDF = filterNewFilesForApp(commonProcessedFilesDF, appId)
        newCommonFilesDF.persist
        commonProcessedFilesDF.unpersist

        // Obtener las máscaras de los archivos de las cargas
        val loadsFilesMasks = config.at("loadMasks".path[Map[String, Seq[String]]]).getOrFail("Could not load config path getLoadsFilesMasks") - "common"

        // Obtener las cargas completadas y los nuevos archivos recibidos
        var areAllLoadsComplete = true
        val completedLoads = loadsFilesMasks.flatMap {
          case (load, loadFileMasks) =>
            val loadFileDatedMasks = replaceDates(loadFileMasks, startDateStr, endDateStr)
            val loadFilesDF = filterByFieldMask(simmProcessedFilesDF, loadFileDatedMasks)
            loadFilesDF.persist
            val loadAndNewFilesDF = if (everyMaskExists(loadFilesDF, loadFileDatedMasks)) {
              val newLoadFilesDF = filterNewFilesForApp(loadFilesDF, appId)
              newLoadFilesDF.persist
              Some(load, newLoadFilesDF)
            } else {
              areAllLoadsComplete = false
              None
            }
            loadFilesDF.unpersist
            loadAndNewFilesDF
        }
        simmProcessedFilesDF.unpersist
info("INFO_OCELOT: se pasa newCommonFilesDF")
newCommonFilesDF.printSchema
        Some(SIMMLoadCompletion(newCommonFilesDF, completedLoads, areAllLoadsComplete,
          config.at("outputGroups".path[Seq[Int]]).getOrFail("Could not load config path getOutputGroups")))
      } else {// No se han procesado todos los archivos comunes, por tanto, no se debe cargar ni generar salidas
        commonProcessedFilesDF.unpersist; simmProcessedFilesDF.unpersist
        None
      }
    }

  /**
    * Carga los datos de la sensibilidad correspondiente en un DataFrame
    * con la estructura SIMM
    *
    * @param sens - nombre de la sensibilidad
    * @param date - fecha de carga
    * @return - DataFrame con la estructura SIMM
    */
  private def loadSensAsSIMM(sens: String, date: Date, perimetroFiltroFechaDF: DataFrame, tradeOperacionesDF: DataFrame,
    plDF: DataFrame, params: Map[String, Dataset[Row]]): DataFrame = {
    // Cargamos y realizamos los filtros correspondientes en ficheros de contrapartidas
    //    val contrapartidasInColumsDF = selectPerimeterCounterparty
    //
    //    // Cargamos tabla de contratos
    //    val contratoDF = repositoryContrato.loadMaxPartition
    //      .select(col("des_xml"))
    //      .filter(col("des_xml").contains("<Type>MUREXID</Type>"))
    //    val contratoInColumsDF = addContratoRdr(contratoDF)
    //
    //    // Añadimos la fecha de filtro para cada una de las contrapartidas
    //    val perimetroFiltroFechaDF = dateFilter(contrapartidasInColumsDF, contratoInColumsDF)

    sens match {
      case "deltair" => loadDeltairAsSIMM(date, perimetroFiltroFechaDF, tradeOperacionesDF, plDF, params)
      case "cred" => loadCreditoAsSIMM(date, perimetroFiltroFechaDF, tradeOperacionesDF, plDF, params)
      case "fx" => loadFxAsSIMM(date, perimetroFiltroFechaDF, tradeOperacionesDF, plDF, params)
      case "vegair" => loadVegaIrAsSIMM(date, perimetroFiltroFechaDF, tradeOperacionesDF, plDF, params)
    }
  }

  /**
    * Cargamos el fichero de contrapartida y filtramos para obtener el perimetro necesario para el CRIF
    *
    * @return DataFrame
    */
  private def selectPerimeterCounterparty(date: Date, params: Map[String, Dataset[Row]]): (DataFrame, DataFrame, DataFrame) = {
    info(s"Cargando la partición máxima de la tabla de contrapartidas...")
    // Cargamos el xml contrapartidas
//    val contrapartidaDF = repositoryContrapartida.loadTable(2018, 10, 31)
    val (specificYear, specificMonth, specificDay) = (2018, 10, 31)
    val contrapartidaDF = params("repositoryContrapartidaDF")
      .where(col("year") === specificYear && col("month") === specificMonth && col("day") === specificDay)
      .select(col("des_xml"))
      .filter(col("des_xml").contains("<GLOBAL>"))

    // Cargamos los valores de codigo de contrapartida, codigo de Mx o Calypso, identificador del origen(Mx o Calypso)
    val contrapartidasColumnarDF = addContrapartidaFromRdr(contrapartidaDF)

    val datePa = getDateParts(date)
    info(s"Cargando la partición máxima de la tabla de perimetro de contrapartidas de calypso...")
    // Cargamos la tabla de calypsoContrapartidas para realizar el filtro de (contrapartidas, cod_entity)
    // que deben persistir
//    val calypsoContrapartidasDF = calypsoContrapartidasSimm.loadMaxPartition

    val calypsoContrapartidasDF = loadMaxPartition(params("calypsoContrapartidasSimmDF"),
      Some(getMaxPartition(spark, config.at("RdCalypso".path[String]).getOrElse(""),"tgsrr_calypso_counterparty")))
      .select(col("cod_shortname"), col("cod_entity"), col("cod_contract_calypso"), col("fec_effectivedate"))

    // hacemos el filtro de las contrpartidas que vienen en el fichero de calypso
    val contrapartidasFiltradasPorCalypsoDF = contrapartidaFiltro(contrapartidasColumnarDF, calypsoContrapartidasDF)

    // Con el filtro anterior recuperamos las contrapartidas de Mx3 para la traducciones posteriores
    val contrapartidaFechaDF = recuperarContrapartidasMx(contrapartidasColumnarDF, contrapartidasFiltradasPorCalypsoDF)
      // Ahora comprobamos con que fecha de filtro nos quedamos, si la que viene en el fichero o la del contrato
      .withColumn("filter_date",
      when(dateConversionUDF(col("fec_effectivedate")) >= lit(filterDate), dateConversionUDF(col("fec_effectivedate")))
        .otherwise(lit(filterDate)))
      .drop(col("fec_effectivedate"))
      .cache

    info(s"Cargando la partición máxima de la tabla de última foto de operaciones de trade repo")
    // Cargamos la tabla de tradeRepoOperaciones para obtener el skeleton, el end_date(fecha_vencimiento) y la fecha de alta
    // Podemos poner todos los origenes de los que deseemos recuperar particion
    // TODO Cambiar valores a traernos de traderepo para el importe nominal

    //los campos imp_nominal_pago e imp_nominal_cobro cambian a string con ",".
    // UDF para transformalos de nuevo a decimal
    val commadot = (s:String) => {
      BigDecimal(s.replaceAll(",","."))
    }
    val commadot_udf = udf(commadot)

//    val tradeDF = repositoryTradeOperaciones.loadMaxPartition.filter((col("cod_aplicacion") === lit("MUREX")) &&
    val tradeDF = loadMaxPartition(params("repositoryTradeOperacionesDF"),
      Some(getMaxPartition(spark, config.at("MdTraderepo".path[String]).getOrElse(""),"treopem_tradeoperaciones_foto")))
      .filter((col("cod_aplicacion") === lit("MUREX")) &&
        col("xti_murex") === lit("3")) //loadTableMultiple("MUREX3EUR")
      .select(col("cod_numoperfront"), col("cod_portfolio"), col("fec_vencimiento"), col("cod_familia"),
        col("cod_grupo"), col("cod_tipo_aut"), col("cod_skeleton_code"), col("fec_alta"), col("cod_tipooperacion"),
        commadot_udf(col("imp_nominal_pago")).as("imp_nominal_pago"),
        commadot_udf(col("imp_nominal_cobro")).as("imp_nominal_cobro"),
        col("cod_divisa_pago"),
        col("cod_divisa_cobro"))
      .withColumn("fec_vencimiento", when(col("fec_vencimiento").isNotNull || col("fec_vencimiento").notEqual(lit("")),
        europeanDateToAmericanDateUDF(col("fec_vencimiento"))).otherwise(col("fec_vencimiento")))
      .dropDuplicates(Seq("cod_numoperfront", "cod_portfolio"))
      .cache

    info(s"Cargando la partición del día de la tabla P&L de risk")
//    val plDF = generalFilters(repositoryPl.loadTable(datePa.year, datePa.month, datePa.day))
    val plDF = generalFilters(params("repositoryPlDF")
      .where(col("year") === datePa.year && col("month") === datePa.month && col("day") === datePa.day))
      .select(col("cod_nb"), col("cod_portfolio"), col("fec_tradedate"), col("imp_mv"))
      .filter("imp_mv != 0")
      //      .dropDuplicates(Seq("cod_nb", "cod_portfolio"))
      .cache

    (contrapartidaFechaDF, tradeDF, plDF)
  }

  /**
    * Transformaciones para obtener los datos de DeltaIr que necesitamos para cargar la tabla SIMM
    * y la salida del CRIF
    *
    * @param date            - Fecha que indica la particion a cargar
    * @param contrapartidaDF - Dataframe de contrapartida para obtener la contrapartida de RDR
    * @return Dataframe con filtros y añadidos necesarios
    */
  private def loadDeltairAsSIMM(date: Date, contrapartidaDF: DataFrame, tradeOperacionesDF: DataFrame,
    plDF: DataFrame, params: Map[String, Dataset[Row]]): DataFrame = {
    info(s"Cargando la partición del día de la tabla deltaIR de risk...")
    // Cargamos la tabla md_gsr.tgsrm_risk_deltair
    val datePa = getDateParts(date)
//    val dfDeltaIrInput = repositoryDeltaIr.loadTable(datePa.year, datePa.month, datePa.day)
    val dfDeltaIrInput = params("repositoryDeltaIrDF")
      .where(s"year=${datePa.year} AND month=${datePa.month} AND day=${datePa.day}")
    val dfFiltrados = generalFilters(dfDeltaIrInput)
    // Hacemos filtro  de fecha con ficheros de trade operaciones y obtener fecha de creacion, end_date y skelet
    val joinWithOperDF = makeJoinWithTradeOperaciones(dfFiltrados, tradeOperacionesDF)
      .withColumn("tradedate_compair", when(col("fec_alta").isNotNull, dateConversionUDF(col("fec_alta")))
        .otherwise(lit("")))

    val addNominalValue = setNominalValue(joinWithOperDF)

    // Añadimos campo de contrapartida de RDR y realizamos el filtro de fecha
    val addCounterPartyDF = makeJoinWithContrapartidas(addNominalValue, contrapartidaDF)
      //Creamos columna para filtrar posteriormente filtrar por cod_version='07' cuando familia-grupo-tipo='IRD-CS-'
      .withColumn("familiaGrupo", concat(col("cod_family"), lit("-"), col("cod_group"), lit("-"), col("cod_type")))
      .persist
    // Realizamos el filtro especifico por cod_version='07' cuando familia-grupo-tipo='IRD-CS-'
    val dfFamilyGroup = addCounterPartyDF.filter((col("familiaGrupo") <=> lit("IRD-CS-")) &&
      (col("cod_version") <=> lit("07")))
    val dfNoFamilyGroup = addCounterPartyDF.filter(col("familiaGrupo") =!= lit("IRD-CS-"))

    val dfFilter07Version = dfFamilyGroup union dfNoFamilyGroup
      .persist

    addCounterPartyDF.unpersist()
    // Llamo a la creacion de la parte SIMM
    val dfSIMMLoadDeltair = deltaIrSIMM(dfFilter07Version)
    // Llamo a la creacion de la parte Schedule
    val dfScheduleDeltair = deltaIrSchedule(dfFilter07Version, plDF)
    dfFilter07Version.unpersist()
    // Devuelvo la union de ambos
    dfSIMMLoadDeltair.union(dfScheduleDeltair)
  }

  private def generalFilters(df: DataFrame): DataFrame = {
    df.filter("cod_entity = '0182' and des_sourcesystem = 'murex3'")
  }

  /**
    * Realiza un join de un df de sensibilidades con la tabla de operaciones para obtener
    * los campos de skelet, fecha de creación y fecha de vencimiento entre otros
    *
    * @param dfSensi       - Dataframe de sensibilidad de Murex 3 o la union de todos ellos
    * @param operacionesDF - Dataframe de trade operaciones
    * @return Dataframe con nuevas columnas de operaciones
    */
  private def makeJoinWithTradeOperaciones(dfSensi: DataFrame, operacionesDF: DataFrame): DataFrame = {
    dfSensi.as("sensibilidad").join(
      operacionesDF.as("operaciones"),
      dfSensi("cod_nb") <=> operacionesDF("cod_numoperfront") &&
        dfSensi("cod_portfolio") <=> operacionesDF("cod_portfolio"))
      .select("sensibilidad.*", "operaciones.fec_vencimiento", "operaciones.cod_skeleton_code",
        "operaciones.fec_alta", "operaciones.cod_tipooperacion", "operaciones.imp_nominal_pago",
        "operaciones.imp_nominal_cobro", "operaciones.cod_divisa_pago", "operaciones.cod_divisa_cobro")
  }

  private def setNominalValue(df: DataFrame): DataFrame = {
    // Funcion udf para devolver el importe nominal correspondiente
    val returnImpNominal: (java.math.BigDecimal, java.math.BigDecimal, String, String) => BigDecimal =
      (imp_nominal_pago: java.math.BigDecimal, imp_nominal_cobro: java.math.BigDecimal,
      div_pago:String,div_cobro: String) => {
        (imp_nominal_cobro, imp_nominal_pago, div_pago, div_cobro) match {
          case (_, _, _, _) if BigDecimal(imp_nominal_cobro) == BigDecimal(0) &&
            BigDecimal(imp_nominal_pago) != BigDecimal(0) =>
            BigDecimal(imp_nominal_pago)
          case (_, _, _, _) if BigDecimal(imp_nominal_cobro) != BigDecimal(0) &&
            BigDecimal(imp_nominal_pago) != BigDecimal(0) =>
            (div_pago, div_cobro) match {
              case (_, _) if div_pago != "EUR" && div_cobro == "EUR" =>
                BigDecimal(imp_nominal_pago)
              case (_, _) if div_pago == "EUR" && div_cobro != "EUR" =>
                BigDecimal(imp_nominal_cobro)
              case (_, _) if div_pago != "EUR" && div_cobro != "EUR" =>
                BigDecimal(imp_nominal_cobro)
              case _ => BigDecimal(0)
            }
          case _ => BigDecimal(0)
        }
      }
    // Registramos la funcion UDF
    val returnImpNominalUDF = udf(returnImpNominal)

    df.withColumn("imp_mv_nominal", returnImpNominalUDF(col("imp_nominal_pago"), col("imp_nominal_cobro"),
      col("cod_divisa_pago"), col("cod_divisa_cobro")))
  }

  /**
    * Realiza un join de un df de sensibilidades con otro de rdr para obtener la traducción en rdr de contrapartidas
    *
    * @param dfSensi - Dataframe de sensibilidad de Murex 3 o la union de todos ellos
    * @param rdrDF   - Dataframe de  rdr contrapartidas
    * @return Dataframe con una nueva columna correspondiente a la contrapartida de RDR
    */
  private def makeJoinWithContrapartidas(dfSensi: DataFrame, rdrDF: DataFrame): DataFrame = {
    dfSensi.as("sensibilidad").join(
      rdrDF.as("rdr"),
      dfSensi("cod_counterparty") <=> rdrDF("key") &&
        dfSensi("cod_entity") <=> rdrDF("cod_entity") &&
        dfSensi("tradedate_compair") >= rdrDF("filter_date"))
      .select("sensibilidad.*", "rdr.value", "rdr.cod_contract_calypso", "rdr.filter_date")
      .drop(col("tradedate_compair"))
  }

  private def deltaIrSIMM(dfDeltaIrInput: DataFrame): DataFrame = {
    // Cargamos la tabla md_gsr.tgsrm_risk_deltair con el importe mayor a 0
    val dfWithoutZeros = dfDeltaIrInput.filter(col("imp_dv01_par_sala_apilog") + col("imp_inflation_par_apilog") =!= 0)
    val dfAddComunFields = addDeltaIrComunFields(dfWithoutZeros)
    val addImModelSIMMDf = dfAddComunFields.withColumn("cod_im_model", lit("SIMM"))

    val dfaddSectorIsdaSIMM = addImModelSIMMDf.withColumn("cod_sector_isda",
      when(col("cod_ircurve").notEqual(""), lit("1"))
        .otherwise(lit(""))
    )

    /*TODO Pendientes de confirmar como decidir que campo elegir entre Libor1m, Libor3m, Libor6m, Libor12m, OIS, Prime
      TODO  para cuando el cod_risk_type= 'Risk_IRCurve'. De momento dejamos OIS
    */
    val dfaddColLabel2SIMM = dfaddSectorIsdaSIMM.withColumn("cod_label2",
      when(col("cod_ircurve").notEqual(""), lit("OIS"))
        .otherwise(lit(""))
    )

    //    val changeFecVencimiento = dfaddColLabel2SIMM.withColumn("fec_vencimiento", lit(""))

    val dfaddRiskTypeSIMM = dfaddColLabel2SIMM.withColumn("cod_risk_type",
      when(col("cod_ircurve").notEqual(""), lit("Risk_IRCurve"))
        .otherwise(lit("Risk_Inflation"))
    )

    val dfAddImpAmountSens = dfaddRiskTypeSIMM.withColumn("imp_amount_sens",
      col("imp_dv01_par_sala_apilog") + col("imp_inflation_par_apilog"))

    val dfaddImppnlvarDeltair = dfAddImpAmountSens.withColumn("imp_pnlvar_deltair",
      when(col("cod_sensi_cross") <=> lit("Direct sensi"), col("imp_dv01_zer_sala"))
        .otherwise(lit(0).cast("decimal(22,8)")))


    // Realizamos un select de los campos que necesitamos para continuar la carga de la tabla SIMM
    selectDF(dfaddImppnlvarDeltair)
  }

  private def addDeltaIrComunFields(deltaIrDF: DataFrame): DataFrame = {
    val dfaddIssuerCode = deltaIrDF.withColumn("cod_issuercode", lit(""))

    val dfaddVolgroupAgrup = dfaddIssuerCode.withColumn("des_volgroup_agrup", lit(""))

    val dfaddCurrpair = dfaddVolgroupAgrup.withColumn("cod_currpair", lit(""))

    val dfaddCodQualifier = dfaddCurrpair.withColumnRenamed("cod_currency", "cod_qualifier")

    val dfaddRiskFactor = dfaddCodQualifier.withColumn("des_riskfactor",
      when(col("cod_ircurve").notEqual(""), col("cod_ircurve"))
        .otherwise(col("cod_curvenameinf"))
    )

    val dfChangeNameDate = dfaddRiskFactor.withColumnRenamed("fec_date_pd", "fec_sens_maturity")

    val dfAddCurrency = dfChangeNameDate.withColumn("cod_curr_amount", lit("EUR"))

    val dfAddUndMaturity = dfAddCurrency.withColumn("qpl_undmaturity", lit(""))

    val dfAddImpAmountCup0 = dfAddUndMaturity.withColumn("imp_amount_cup0",
      col("imp_dv01_zer_sala_apilog") + col("imp_inflation_zero_apilog"))

    val dfaddimppnlvarvegair = dfAddImpAmountCup0.withColumn("imp_pnlvar_vegair",
      lit(0).cast("decimal(22,8)"))

    val createDivMapnulo = udf(() => scala.collection.mutable.Map("" -> BigDecimal(0)))
    //val createDivMapnulo = udf(()=> Map(""-> BigDecimal(0)))
    val dfaddimppnlvarvdeltafx = dfaddimppnlvarvegair.withColumn("imp_pnlvar_deltafx", createDivMapnulo())


    dfaddimppnlvarvdeltafx.withColumn("des_sourcename", lit("DeltaIR"))
  }

  /**
    * Realizamos select sobre un dataframe de sensibilidad de Murex 3 para obtener las columnas
    * necesarias para continuar la carga de la tabla SIMM
    *
    * @param df Dataframe de sensibilidad de Murex 3 sobre el que realizar el select
    * @return Nuevo dataframe con las columnas que necesitamos en la carga de la tabla SIMM
    */
  private def selectDF(df: DataFrame): DataFrame = {
    df.select(col("cod_entity"), col("cod_portfolio"), col("cod_nb"), col("fec_tradedate"), col("des_volgroup_agrup"),
      col("value").as("cod_shortname"), col("cod_issuercode"), col("cod_family"), col("cod_group"), col("cod_currpair"),
      col("cod_type"), col("cod_risk_type"), col("cod_qualifier"), col("cod_sector_isda"), col("des_riskfactor"),
      col("cod_label2"), col("cod_curr_amount"), col("des_sourcename"), col("imp_amount_sens"), col("imp_amount_cup0"),
      col("des_sourcesystem"), col("cod_counterparty"), col("fec_sens_maturity"), col("fec_horizondate"),
      col("cod_strategy"), col("qpl_undmaturity"), col("cod_contract_calypso"), col("filter_date"),
      col("fec_vencimiento"), col("fec_alta"), col("cod_im_model"), col("cod_tipooperacion"), col("cod_skeleton_code"),
      col("imp_pnlvar_deltair"), col("imp_pnlvar_vegair"), col("imp_pnlvar_deltafx"),
      col("year"), col("month"), col("day"))
      .dropDuplicates(Seq("cod_entity", "cod_portfolio", "cod_nb", "fec_tradedate", "des_volgroup_agrup",
        "cod_shortname", "cod_issuercode", "cod_family", "cod_group", "cod_currpair",
        "cod_type", "cod_risk_type", "cod_qualifier", "cod_sector_isda", "des_riskfactor",
        "cod_label2", "cod_curr_amount", "des_sourcename", "imp_amount_sens", "imp_amount_cup0",
        "des_sourcesystem", "cod_counterparty", "fec_sens_maturity", "fec_horizondate",
        "cod_strategy", "qpl_undmaturity", "cod_contract_calypso", "filter_date",
        "fec_vencimiento", "fec_alta", "cod_im_model", "cod_tipooperacion", "cod_skeleton_code",
        "imp_pnlvar_deltair", "imp_pnlvar_vegair",
        "year", "month", "day"))

  }

  private def deltaIrSchedule(dfDeltaIrInput: DataFrame, plDF: DataFrame): DataFrame = {
    // Realizamos el join con la tabla de P&L para obtener el imp_mv de la boleta, portfolio correspondiente
    val plDFNoFecTradeDate = plDF.select(col("cod_nb"), col("cod_portfolio"), col("imp_mv"))
    val addMarketValue = makeJoinWithPL(dfDeltaIrInput, plDFNoFecTradeDate)
    // Cargamos la tabla md_gsr.tgsrm_risk_deltair con el importe mayor a 0
    val dfWithoutZeros = addMarketValue.filter(col("imp_dv01_par_sala_apilog") + col("imp_inflation_par_apilog") =!= 0)
    //&&
    //        (col("imp_mv") !== 0))
    val dfAddComunFields = addDeltaIrComunFields(dfWithoutZeros)
    val addImModelSIMMDf = dfAddComunFields.withColumn("cod_im_model", lit("Schedule"))

    // añadimos campo nulo imp_pnlvar_deltair para calcular posteriormente var
    val dfaddImppnlvarDeltair = addImModelSIMMDf.withColumn("imp_pnlvar_deltair", lit(0).cast("decimal(22,8)"))

    val dfaddSectorIsdaSchedule = dfaddImppnlvarDeltair.withColumn("cod_sector_isda", lit(""))

    val dfaddColLabel2Schedule = dfaddSectorIsdaSchedule.withColumn("cod_label2", lit("")).persist

    val dfaddRiskTypeSchedulePV = dfaddColLabel2Schedule.withColumn("cod_risk_type", lit("PV"))
      //      .filter(col("imp_mv") !== 0)
      .withColumn("imp_amount_sens", col("imp_mv"))
    val dfaddRiskTypeScheduleNotional = dfaddColLabel2Schedule.withColumn("cod_risk_type", lit("Notional"))
      .withColumn("imp_amount_sens", col("imp_mv_nominal"))
    //      .filter(col("imp_dv01_par_sala_apilog") + col("imp_inflation_par_apilog") !== 0)
    //      .withColumn("imp_amount_sens", col("imp_dv01_par_sala_apilog") + col("imp_inflation_par_apilog"))

    dfaddColLabel2Schedule.unpersist
    // Realizamos un select de los campos que necesitamos para continuar la carga de la tabla SIMM
    selectDF(dfaddRiskTypeSchedulePV) union selectDF(dfaddRiskTypeScheduleNotional)
  }


  /**
    * Realiza un join de un df de sensibilidades con pl para obtener el valor de mercado de la boleta
    *
    * @param dfSensi - Dataframe de sensibilidad de Murex 3
    * @param plDF    - Dataframe de P&L
    * @return Dataframe con una nueva columna correspondiente al importe de mercado
    */
  private def makeJoinWithPL(dfSensi: DataFrame, plDF: DataFrame): DataFrame = {
    dfSensi.as("sensibilidad").join(
      plDF.as("pl"),
      Seq("cod_nb", "cod_portfolio"))
      .select("sensibilidad.*", "pl.imp_mv")
  }

  /**
    * Transformaciones para obtener los datos de Credito que necesitamos para cargar la tabla SIMM
    * y la salida del CRIF
    *
    * @param date            - Fecha que indica la particion a cargar
    * @param contrapartidaDF Dataframe de contrapartida para obtener la contrapartida de RDR
    * @return Dataframe con filtros y añadidos necesarios
    */
  private def loadCreditoAsSIMM(date: Date, contrapartidaDF: DataFrame, tradeOperacionesDF: DataFrame,
    plDF: DataFrame, params: Map[String, Dataset[Row]]): DataFrame = {
    info(s"Cargando la partición del día de la tabla de credito de risk...")
    // Cargamos la tabla md_gsr.tgsrm_risk_cred
    val datePa = getDateParts(date)
//    val dfDeltaCredInput = repositoryCred.loadTable(datePa.year, datePa.month, datePa.day)
    val dfDeltaCredInput = params("repositoryCredDF")
      .where(s"year=${datePa.year} AND month=${datePa.month} AND day=${datePa.day}")
    val dfFiltrados = generalFilters(dfDeltaCredInput)
    // Hacemos filtro  de fecha con ficheros de trade operaciones y obtener fecha de creacion, end_date y skelet
    val joinWithOperDF = makeJoinWithTradeOperaciones(dfFiltrados, tradeOperacionesDF)
      .withColumn("tradedate_compair", when(col("fec_alta").isNotNull, dateConversionUDF(col("fec_alta")))
        .otherwise(lit("")))

    val addNominalValue = setNominalValue(joinWithOperDF)

    // Añadimos campo de contrapartida de RDR y realizamos el filtro de fecha
    val addCounterPartyDF = makeJoinWithContrapartidas(addNominalValue, contrapartidaDF)
      .persist
    //Llamo a la creacion de la parte SIMM
    val dfSIMMLoadCred = credSIMM(addCounterPartyDF)
    //Llamo a la creacion de la parte Schedule
    val dfScheduleCred = credSchedule(addCounterPartyDF, plDF)
    addCounterPartyDF.unpersist()
    //Devuelvo la union de ambos
    dfSIMMLoadCred.union(dfScheduleCred)
  }

  private def credSIMM(dfCredInput: DataFrame): DataFrame = {
    // Cargamos la tabla md_gsr.tgsrm_risk_deltair con el importe mayor a 0
    val dfWithoutZeros = dfCredInput.filter(col("imp_cr01_par") =!= 0)
    val dfAddComunFields = addCredComunFields(dfWithoutZeros)

    val addImModelSIMMDf = dfAddComunFields.withColumn("cod_im_model", lit("SIMM"))

    //TODO de momento constante
    val dfaddSectorIsdaSIMM = addImModelSIMMDf.withColumn("cod_sector_isda", lit("1"))

    // TODO Confirmar en caso de divisa_pago vacía
    val dfaddColLabel2SIMM = dfaddSectorIsdaSIMM.withColumn("cod_label2", col("cod_divisa_pago"))

    val dfaddRiskTypeSIMM = dfaddColLabel2SIMM.withColumn("cod_risk_type", lit("Risk_CreditQ"))

    val dfAddImpAmountSens = dfaddRiskTypeSIMM.withColumn("imp_amount_sens", col("imp_cr01_par"))

    // Realizamos un select de los campos que necesitamos para continuar la carga de la tabla SIMM
    selectDF(dfAddImpAmountSens)
  }

  private def addCredComunFields(dfCred: DataFrame): DataFrame = {
    val dfaddCodQualifier = dfCred.withColumn("cod_qualifier", concat(lit("ISIN:"), col("cod_seccode")))

    val dfaddVolgroupAgrup = dfaddCodQualifier.withColumn("des_volgroup_agrup", lit(""))

    val dfaddCurrpair = dfaddVolgroupAgrup.withColumn("cod_currpair", lit(""))

    val dfaddRiskFactor = dfaddCurrpair.withColumnRenamed("des_curvename", "des_riskfactor")

    val dfChangeNameDate = dfaddRiskFactor.withColumnRenamed("fec_date_p", "fec_sens_maturity")

    val dfAddCurrency = dfChangeNameDate.withColumn("cod_curr_amount", lit("EUR"))

    val dfAddUndMaturity = dfAddCurrency.withColumn("qpl_undmaturity", lit(""))

    val dfAddSourcename = dfAddUndMaturity.withColumn("des_sourcename", lit("DeltaCred"))

    // añadimos campo nulo imp_pnlvar_deltair para calcular posteriormente var
    val dfaddImppnlvarDeltair = dfAddSourcename.withColumn("imp_pnlvar_deltair", lit(0).cast("decimal(22,8)"))

    // añadimos campo nulo imp_pnlvar_vegair para calcular posteriormente var
    val dfaddImppnlvarvegair = dfaddImppnlvarDeltair.withColumn("imp_pnlvar_vegair", lit(0).cast("decimal(22,8)"))

    // añadimos campo nulo imp_pnlvar_deltafx para calcular posteriormente var
    val createDivMapnulo = udf(() => scala.collection.mutable.Map("" -> BigDecimal(0)))
    val dfaddImppnlvardeltafx = dfaddImppnlvarvegair.withColumn("imp_pnlvar_deltafx", createDivMapnulo())

    dfaddImppnlvardeltafx.withColumn("imp_amount_cup0", lit(0).cast("decimal(22,8)"))

  }

  private def credSchedule(dfDeltaIrInput: DataFrame, plDF: DataFrame): DataFrame = {
    // Realizamos el join con la tabla de P&L para obtener el imp_mv de la boleta, portfolio correspondiente
    val plDFNoFecTradeDate = plDF.select(col("cod_nb"), col("cod_portfolio"), col("imp_mv"))
    val addMarketValue = makeJoinWithPL(dfDeltaIrInput, plDFNoFecTradeDate)
    // Cargamos la tabla md_gsr.tgsrm_risk_deltair con el importe mayor a 0
    val dfWithoutZeros = addMarketValue.filter(col("imp_cr01_par") =!= 0)
    // && (col("imp_mv") !== 0))
    val dfAddComunFields = addCredComunFields(dfWithoutZeros)
    val addImModelSIMMDf = dfAddComunFields.withColumn("cod_im_model", lit("Schedule"))

    val dfaddSectorIsdaSchedule = addImModelSIMMDf.withColumn("cod_sector_isda", lit(""))

    val dfaddColLabel2Schedule = dfaddSectorIsdaSchedule.withColumn("cod_label2", lit(""))
      .cache

    val dfaddRiskTypeSchedulePV = dfaddColLabel2Schedule.withColumn("cod_risk_type", lit("PV"))
      //      .filter(col("imp_mv") !== 0)
      .withColumn("imp_amount_sens", col("imp_mv"))
    val dfaddRiskTypeScheduleNotional = dfaddColLabel2Schedule.withColumn("cod_risk_type", lit("Notional"))
      .withColumn("imp_amount_sens", col("imp_mv_nominal"))
    //      .filter(col("imp_cr01_par") !== 0)
    //      .withColumn("imp_amount_sens", col("imp_cr01_par"))

    dfaddColLabel2Schedule.unpersist

    // Realizamos un select de los campos que necesitamos para continuar la carga de la tabla SIMM
    selectDF(dfaddRiskTypeSchedulePV) union selectDF(dfaddRiskTypeScheduleNotional)
  }

  /**
    * Transformaciones para obtener los datos de deltaFX y vegaFX que necesitamos para cargar la tabla SIMM
    * y la salida del CRIF
    *
    * @param date            - Fecha que indica la particion a cargar
    * @param contrapartidaDF Dataframe de contrapartida para obtener la contrapartida de RDR
    * @return Dataframe, union de deltaFX y vegaFX con filtros y añadidos necesarios
    */
  private def loadFxAsSIMM(date: Date, contrapartidaDF: DataFrame, tradeOperacionesDF: DataFrame,
    plDF: DataFrame, params: Map[String, Dataset[Row]]): DataFrame = {
    info(s"Cargando la partición del día de la tabla de fx de risk...")
    // Cargamos la tabla md_gsr.tgsrm_risk_fx
    val datePa = getDateParts(date)
    val dfFxInput = params("repositoryDeltaFxDF")
      .where(s"year=${datePa.year} AND month=${datePa.month} AND day=${datePa.day}")
    val dfFiltrados = generalFilters(dfFxInput)
    // Hacemos filtro  de fecha con ficheros de trade operaciones y obtener fecha de creacion, end_date y skelet
    val joinWithOperDF = makeJoinWithTradeOperaciones(dfFiltrados, tradeOperacionesDF)
      .withColumn("tradedate_compair", when(col("fec_alta").isNotNull, dateConversionUDF(col("fec_alta")))
        .otherwise(lit("")))

    val addNominalValue = setNominalValue(joinWithOperDF)

    // Añadimos campo de contrapartida de RDR y realizamos el filtro de fecha
    val addCounterPartyDF = makeJoinWithContrapartidas(addNominalValue, contrapartidaDF)
      .persist
    //Llamo a la creacion de la parte SIMM
    val dfSIMMLoadFx = fxSIMM(addCounterPartyDF)
    // Llamo a la creacion de la parte Schedule
    val dfScheduleFx = fxSchedule(addCounterPartyDF, plDF)
    addCounterPartyDF.unpersist()
    // Devuelvo la union de ambos
    dfSIMMLoadFx.union(dfScheduleFx)
  }

  private def fxSIMM(dfFxInput: DataFrame): DataFrame = {
    // Cargamos la tabla md_gsr.tgsrm_risk_fx con el importe mayor a 0
    val dfWithoutZeros = dfFxInput.filter((col("imp_fx_delta_apilog") =!= 0) || (col("imp_fx_vega_apilog") =!= 0))
    val dfAddComunFields = addFxComunFields(dfWithoutZeros)
    val addImModelSIMMDf = dfAddComunFields.withColumn("cod_im_model", lit("SIMM"))


    // añado imp_pnlvar_deltafx: importe de exposición en euros a divisa para el cálculo del var.
    // Map (cod_c1 -> Exp a divisa1) (cod_c2 -> exp a divisa 2). Para ello usamos una UDF: createDivMap
    // filtro solo aquellos que en la SIMM tienen deltafx , al resto, los que tienen VegaFX, tendrá 0.
    val createDivMap = udf((codc1: String, imppl: java.math.BigDecimal, codc2: String, impopt: java.math.BigDecimal) =>
      (codc1: String, imppl: java.math.BigDecimal, codc2: String, impopt: java.math.BigDecimal) match {
        case (null, _, null, _) => scala.collection.mutable.Map("" -> BigDecimal(0))
        case (_, _, null, _) => scala.collection.mutable.Map(codc1 -> BigDecimal(imppl))
        case (null, _, _, _) => scala.collection.mutable.Map(codc2 -> BigDecimal(imppl))
        case (_, _, _, _) => scala.collection.mutable.Map(codc1 -> BigDecimal(imppl)).+=(codc2 -> BigDecimal(impopt))
      }
    )
    val createDivMapnulo = udf(() => scala.collection.mutable.Map("" -> BigDecimal(0)))
    val dfaddImpPnlvarfilterDeltafx = addImModelSIMMDf.filter(col("cod_risk_type") === lit("Risk_FX"))
      .withColumn("imp_pnlvar_deltafx",
        createDivMap(col("cod_c1"), col("imp_expfx_pl_apilog"), col("cod_c2"), col("imp_expfx_opt_apilog")))

    val dfaddImpPnlvarfiltervegafx = addImModelSIMMDf.filter(col("cod_risk_type") =!= lit("Risk_FX"))
      .withColumn("imp_pnlvar_deltafx", createDivMapnulo())

    // hago unionall de las dos variables anteriores
    val dfaddImpPnlvarDeltafx = dfaddImpPnlvarfilterDeltafx.union(dfaddImpPnlvarfiltervegafx)

    // Realizamos un select de los campos que necesitamos para continuar la carga de la tabla SIMM
    selectDF(dfaddImpPnlvarDeltafx)
  }

  private def addFxComunFields(dfFx: DataFrame): DataFrame = {
    val dfaddIssuerCode = dfFx.withColumn("cod_issuercode", lit(""))

    val dfaddVolgroupAgrup = dfaddIssuerCode.withColumn("des_volgroup_agrup", lit(""))

    val dfaddCodQualifier = dfaddVolgroupAgrup.withColumn("cod_qualifier", col("cod_c1"))

    //// Se cambia por el correo de calypso sobre cambios según valor del risk_type
    val dfaddSectorIsda = dfaddCodQualifier.withColumn("cod_sector_isda", lit(""))

    val dfaddRiskFactor = dfaddSectorIsda.withColumn("des_riskfactor", col("cod_c1"))

    val dfChangeNameDate = dfaddRiskFactor.withColumn("fec_sens_maturity", lit(""))

    // Se cambia por el correo de calypso sobre cambios según valor del risk_type
    val dfaddColLabel2 = dfChangeNameDate.withColumn("cod_label2", lit(""))

    val dfAddCurrency = dfaddColLabel2.withColumn("cod_curr_amount", lit("EUR"))

    val dfAddUndMaturity = dfAddCurrency.withColumn("qpl_undmaturity", lit(""))
    // añadimos campo nulo imp_pnlvar_deltair para calcular posteriormente var
    val dfaddImppnlvarDeltair = dfAddUndMaturity.withColumn("imp_pnlvar_deltair", lit(0).cast("decimal(22,8)"))

    // añadimos campo nulo imp_pnlvar_vegair para calcular posteriormente var
    val dfaddImppnlvarvegair = dfaddImppnlvarDeltair.withColumn("imp_pnlvar_vegair", lit(0).cast("decimal(22,8)"))

    val dfAddImpAmountCup0 = dfaddImppnlvarvegair.withColumn("imp_amount_cup0", lit(0).cast("decimal(22,8)"))
      .cache

    // Desdoblamos en dos dataframe diferentes los campos diferentes que tienen en la SIMM DeltaFX y VegaFX
    val dfaddRiskTypeDelta = dfAddImpAmountCup0.withColumn("cod_risk_type", lit("Risk_FX"))
    val dfaddRiskTypeVega = dfAddImpAmountCup0.withColumn("cod_risk_type", lit("Risk_FXVol"))

    val dfAddSourcenameDelta = dfaddRiskTypeDelta.withColumn("des_sourcename", lit("DeltaFx"))
    val dfAddSourcenameVega = dfaddRiskTypeVega.withColumn("des_sourcename", lit("VegaFX"))

    val dfAddImpAmountSensDelta = dfAddSourcenameDelta.withColumn("imp_amount_sens", col("imp_fx_delta_apilog"))
      .filter(col("imp_amount_sens") =!= 0)
    val dfAddImpAmountSensVega = dfAddSourcenameVega.withColumn("imp_amount_sens", col("imp_fx_vega_apilog"))
      .filter(col("imp_amount_sens") =!= 0)

    dfAddImpAmountCup0.unpersist()

    dfAddImpAmountSensDelta.union(dfAddImpAmountSensVega)
  }

  private def fxSchedule(dfFxInput: DataFrame, plDF: DataFrame): DataFrame = {
    // Realizamos el join con la tabla de P&L para obtener el imp_mv de la boleta y portfolio correspondiente
    val plDFNoFecTradeDate = plDF.select(col("cod_nb"), col("cod_portfolio"), col("imp_mv"))
    val addMarketValue = makeJoinWithPL(dfFxInput, plDFNoFecTradeDate)
    // Cargamos la tabla md_gsr.tgsrm_risk_deltair con el importe mayor a 0
    val dfWithoutZeros = addMarketValue.filter(
      (col("imp_fx_delta_apilog") =!= 0) || (col("imp_fx_vega_apilog") =!= 0))
    // && (col("imp_mv") !== 0))
    val dfAddComunFields = addFxComunFields(dfWithoutZeros)
    // añadimos imp_pnlvar_deltafx  igual a 0.
    val createDivMapnulo = udf(() => scala.collection.mutable.Map("" -> BigDecimal(0)))
    val dfaddImpPnlvardeltafx = dfAddComunFields.withColumn("imp_pnlvar_deltafx", createDivMapnulo())
    val addImModelScheduleDf = dfaddImpPnlvardeltafx.withColumn("cod_im_model", lit("Schedule"))
      .persist

    val dfaddRiskTypeSchedulePV = addImModelScheduleDf.withColumn("cod_risk_type", lit("PV"))
      .withColumn("imp_amount_sens", col("imp_mv"))
    val dfaddRiskTypeScheduleNotional = addImModelScheduleDf.withColumn("cod_risk_type", lit("Notional"))
      .withColumn("imp_amount_sens", col("imp_mv_nominal"))

    addImModelScheduleDf.unpersist
    // Realizamos un select de los campos que necesitamos para continuar la carga de la tabla SIMM
    selectDF(dfaddRiskTypeSchedulePV) union selectDF(dfaddRiskTypeScheduleNotional)
  }

  /**
    * Transformaciones para obtener los datos de VegaIR que necesitamos para cargar la tabla SIMM
    * y la salida del CRIF
    *
    * @param date            - Fecha que indica la particion a cargar
    * @param contrapartidaDF Dataframe de contrapartida para obtener la contrapartida de RDR
    * @return Dataframe con filtros y añadidos necesarios
    */
  private def loadVegaIrAsSIMM(date: Date, contrapartidaDF: DataFrame, tradeOperacionesDF: DataFrame,
    plDF: DataFrame, params: Map[String, Dataset[Row]]): DataFrame = {
    info(s"Cargando la partición del día de la tabla vegaIR de risk...")
    // Cargamos la tabla md_gsr.tgsrm_risk_vegair
    val datePa = getDateParts(date)
//    val dfvegaIr = repositoryVegaIr.loadTable(datePa.year, datePa.month, datePa.day)
    val dfvegaIr = params("repositoryVegaIrDF").where(s"year=${datePa.year} AND month=${datePa.month} AND day=${datePa.day}")
    val dfFiltrados = generalFilters(dfvegaIr)
    // Hacemos filtro  de fecha con ficheros de trade operaciones y obtener fecha de creacion, end_date y skelet
    val joinWithOperDF = makeJoinWithTradeOperaciones(dfFiltrados, tradeOperacionesDF)
      .withColumn("tradedate_compair", when(col("fec_alta").isNotNull, dateConversionUDF(col("fec_alta")))
        .otherwise(lit("")))
    val addNominalValue = setNominalValue(joinWithOperDF)
    val addTradedateAndImpMV = makeJoinWithPLWithTradeDate(addNominalValue, plDF)
    // Añadimos campo de contrapartida de RDR y realizamos el filtro de fecha
    val addCounterPartyDF = makeJoinWithContrapartidas(addTradedateAndImpMV, contrapartidaDF)
      .persist
    //Llamo a la creacion de la parte SIMM
    val dfSIMMLoadVegaIr = vegairSIMM(addCounterPartyDF, plDF, datePa)
    //Llamo a la creacion de la parte Schedule
    val dfScheduleVegaIr = vegairSchedule(addCounterPartyDF, plDF)
    addCounterPartyDF.unpersist()
    //Devuelvo la union de ambos
    dfSIMMLoadVegaIr.union(dfScheduleVegaIr)
  }

  /**
    * Realiza un join de un df de sensibilidades con pl para obtener el valor de mercado de la boleta y
    * la fecha de operacion
    *
    * @param dfSensi - Dataframe de sensibilidad de Murex 3
    * @param plDF    - Dataframe de P&L
    * @return Dataframe con una nueva columna correspondiente al importe de mercado
    */
  private def makeJoinWithPLWithTradeDate(dfSensi: DataFrame, plDF: DataFrame): DataFrame = {
    dfSensi.as("sensibilidad").join(
      plDF.as("pl"),
      Seq("cod_nb", "cod_portfolio"))
      .select("sensibilidad.*", "pl.imp_mv", "pl.fec_tradedate")
  }

  private def vegairSIMM(dfVegairInput: DataFrame, plDF: DataFrame, datePa: JavaDateParts): DataFrame = {
    // Cargamos la tabla md_gsr.tgsrm_risk_vegair con el importe mayor a 0
    val dfWithoutZeros = dfVegairInput.filter(
      col("imp_normal_vega_apilog") + col("imp_inflvegazc_apilog") + col("imp_vega_yield_apilog") =!= 0)

    val dfAddComunFields = addVegairComunFields(dfWithoutZeros, plDF)

    val addImModelSIMMDf = dfAddComunFields.withColumn("cod_im_model", lit("SIMM"))

    val dfaddRiskTypeSIMM = addImModelSIMMDf.withColumn("cod_risk_type", lit("Risk_IRVol"))

    val dfAddImpAmountSens = dfaddRiskTypeSIMM.withColumn("imp_amount_sens",
      col("imp_normal_vega_apilog") + col("imp_inflvegazc_apilog") + col("imp_vega_yield_apilog"))

    // Añadimos campos para el calculo de var : Importe de VegaIR sin inflacion para el cálculo del Var
    val dfAddimppnlvarvegair = dfAddImpAmountSens.withColumn("imp_pnlvar_vegair", col("imp_normal_vega") + col("imp_vega_yield"))


    // Realizamos un select de los campos que necesitamos para continuar la carga de la tabla SIMM
    selectDF(dfAddimppnlvarvegair)
  }

  private def addVegairComunFields(vegaIrDf: DataFrame, plDF: DataFrame): DataFrame = {

    //    val addMarketValue = makeJoinWithPLWithTradeDate(vegaIrDf, plDF)

    val dfaddIssuerCode = vegaIrDf.withColumn("cod_issuercode", lit(""))

    val dfaddCurrpair = dfaddIssuerCode.withColumn("cod_currpair", lit(""))

    val dfaddCodQualifier = dfaddCurrpair.withColumn("cod_qualifier", col("cod_currency"))

    // Cambiamos a vacio por requerimientos funcionales, tras correo de Calypso
    val dfaddSectorIsda = dfaddCodQualifier.withColumn("cod_sector_isda", lit(""))

    // TODO Por definir. Parece que cruza por la columna des_volgroup
    val dfaddRiskFactor = dfaddSectorIsda.withColumnRenamed("des_volgroup", "des_riskfactor")

    val dfChangeNameDate = dfaddRiskFactor.withColumnRenamed("qpl_opt_maturity", "fec_sens_maturity")

    // Cambiamos a vacio por requerimientos funcionales, tras correo de Calypso
    val dfaddColLabel2 = dfChangeNameDate.withColumn("cod_label2", lit(""))

    val dfAddCurrency = dfaddColLabel2.withColumn("cod_curr_amount", lit("EUR"))

    val dfAddSourcename = dfAddCurrency.withColumn("des_sourcename", lit("VegaIR"))
    // añadimos campo nulo imp_pnlvar_deltair para calcular posteriormente var
    val dfaddImppnlvarDeltair = dfAddSourcename.withColumn("imp_pnlvar_deltair", lit(0).cast("decimal(22,8)"))
    // añadimos campo nulo imp_pnlvar_deltafx para calcular posteriormente var
    val createDivMapnulo = udf(() => scala.collection.mutable.Map("" -> BigDecimal(0)))
    val dfaddImppnlvarDeltafx = dfaddImppnlvarDeltair.withColumn("imp_pnlvar_deltafx", createDivMapnulo())


    dfaddImppnlvarDeltafx.withColumn("imp_amount_cup0", lit(0).cast("decimal(22,8)"))

  }

  private def vegairSchedule(dfVegaIrInput: DataFrame, plDF: DataFrame): DataFrame = {

    val dfAddComunFields = addVegairComunFields(dfVegaIrInput, plDF)

    val dfWithoutZeros = dfAddComunFields.filter(
      col("imp_normal_vega_apilog") + col("imp_inflvegazc_apilog") + col("imp_vega_yield_apilog") =!= 0)

    // Añadimos columna de imp_pnlvar_vegair  con 0s
    val dfaddimppnlvarvegair = dfWithoutZeros.withColumn("imp_pnlvar_vegair", lit(0).cast("decimal(22,8)"))

    val addImModelSIMMDf = dfaddimppnlvarvegair.withColumn("cod_im_model", lit("Schedule"))
      .cache

    val dfaddRiskTypeSchedulePV = addImModelSIMMDf.withColumn("cod_risk_type", lit("PV"))
      .withColumn("imp_amount_sens", col("imp_mv"))
    val dfaddRiskTypeScheduleNotional = addImModelSIMMDf.withColumn("cod_risk_type", lit("Notional"))
      .withColumn("imp_amount_sens", col("imp_mv_nominal"))
    //      .filter(col("imp_normal_vega_apilog") + col("imp_inflvegazc_apilog") + col("imp_vega_yield_apilog") !== 0)
    //      .withColumn("imp_amount_sens",
    //        col("imp_normal_vega_apilog") + col("imp_inflvegazc_apilog") + col("imp_vega_yield_apilog"))

    addImModelSIMMDf.unpersist

    // Realizamos un select de los campos que necesitamos para continuar la carga de la tabla SIMM
    selectDF(dfaddRiskTypeSchedulePV) union selectDF(dfaddRiskTypeScheduleNotional)
  }


  /**
    * Aplica filtros, tranformaciones genéricas, bucketización y finalmente guarda en la tabla de SIMM.
    * Adicionalmente se informará de boletas sin regla ni excepción
    *
    * @param unifiedSIMMDF - Union de los dataframes con formato SIMM de las sensibilidades de Murex 3
    * @return - opcional, objeto que indica qué cargas se han completado
    */
  private def processUnifiedSIMMDF(unifiedSIMMDF: DataFrame, date: Date, params: Map[String, Dataset[Row]]): Map[String, DataFrame]  = {
    // Aplicar filtros y transformaciones comunes

    val distictNBDF = unifiedSIMMDF.select(col("cod_nb")).distinct
      .withColumn("cod_prod_rtce", ((rand * 150) + 82).cast("int"))
      .cache
    info("INFO_OCELOTE 101: " + distictNBDF.count)
    val filterEntityAndSourceSystemDF = unifiedSIMMDF.join(distictNBDF, "cod_nb")
      .filter("cod_entity = '0182' and des_sourcesystem = 'murex3' ")
      .cache
    info("INFO_OCELOTE 102: " + filterEntityAndSourceSystemDF.count)
    distictNBDF.unpersist

    // Cargar la tabla de equivalencias de productClass
    // Añadimos el distinct, a pesar de los retrasos por shuffles, ya que si tuvieramos duplicados
    // el join múltiplica los tiempo de forma exponencial
    info(s"Cargando la máxima partición de la tabla de Mentor de productos...")
//    val productClassDF = repositoryProductClassSimm.loadMaxPartition
    val productClassDF = loadMaxPartition(params("repositoryProductClassSimmDF"),
      Some(getMaxPartition(spark, config.at("RdMentor".path[String]).getOrElse(""),"tgsrr_mentor_products")))
      .select(col("xti_isim"), col("qnu_weight"), col("cod_rtceproductid"),
        col("xti_credit_risk"), col("cod_product_class"), col("cod_product_class_schedule")) // col("cod_rtceproductid"))
      .cache
    info("INFO_OCELOTE 103: "+ productClassDF.count)
    // Dividimos entre productos con estrategia o sin ella
    val unificatedWithStrategy = filterEntityAndSourceSystemDF.filter(col("cod_strategy").like("ST-%"))
    info("INFO_OCELOTE 104: " + unificatedWithStrategy.count)
    val unificatedWithOUTStrategy = filterEntityAndSourceSystemDF.filter(!col("cod_strategy").like("ST-%"))
    info("INFO_OCELOTE 105: " + unificatedWithOUTStrategy.count)
    // Añadimos diferentes products_class y productos rtce dominante para los productos con estrategia
    val productsWithStrategyDF = calculateProductsWithStrategy(unificatedWithStrategy, productClassDF)
    info("INFO_OCELOTE 106: " + productsWithStrategyDF.count)
    // Añadimos diferentes products_class y productos rtce dominante para los productos sin estrategia
    val productsWithOUTStrategyDF = calculateProductsWithOUTStrategy(unificatedWithOUTStrategy, productClassDF)
    info("INFO_OCELOTE 107: " + productsWithOUTStrategyDF.count)

    val addProductclassDF = productsWithStrategyDF.union(productsWithOUTStrategyDF)
    info("INFO_OCELOTE 108: " + addProductclassDF.count)

    // Realizamos la carga de la tabla tgsrr_algorithmics_portftrida para realizar un filtro por portfolios
    // Nos quedamos con los portfolios que tienen un 0 en el campo xti_credexclonl
    info(s"Cargando la partición del dia de la tabla de tgsrr_algorithmics_portftrida...")
    val datePa: JavaDateParts = getDateParts(date)
//    val loadPortfoliosTrida = repositoryPortfTrida.loadTable(datePa.year, datePa.month, datePa.day)
    val loadPortfoliosTrida = params("repositoryPortfTridaDF").where(s"year=${datePa.year} AND month=${datePa.month} AND day=${datePa.day}")
      .filter(col("xti_credexclonl") === "0")
      .select(col("des_basicunit").as("cod_portfolio"))
      .distinct
    info("INFO_OCELOTE 109: " + loadPortfoliosTrida.count)

    // Los portfolios obtenidos de la tabla tgsrr_algorithmics_portftrida nos marcan el perimetro de la tabla SIMM
    // Realizamos el join con la tabla que tenemos hasta ahora para eliminar los portfolios que no aparecen
    val filterPrtfoliosTrida = addProductclassDF
      .join(loadPortfoliosTrida, "cod_portfolio")
    info("INFO_OCELOTE 110: " + filterPrtfoliosTrida.count)

    // Realizamos la carga de la tabla tgsrm_triresolve para añadir el campo match_id
    info(s"Cargando la partición del dia de la tabla de tgsrm_triresolve...")
//    val loadTriresolve = repositoryMasterTriresolve.loadTable(datePa.year, datePa.month, datePa.day)
    val loadTriresolve = params("repositoryMasterTriresolveDF").where(s"year=${datePa.year} AND month=${datePa.month} AND day=${datePa.day}")
      .select(col("cod_match_id"), col("cod_nb"))
    info("INFO_OCELOTE 111: " + loadTriresolve.count)

    // Realizamos el join por cod_nb(número de boleta) y nos quedamos el match_id
    val addMatchId = filterPrtfoliosTrida
      .join(loadTriresolve, "cod_nb")
    info("INFO_OCELOTE 112: " + loadTriresolve.count)

    // Si se necesitan finalmente se recogeran de RDR o del fichero de CALYPSO
    val addPostRegulationDf = addMatchId.withColumn("cod_post_regulations", lit("ESA"))
    // Si se necesitan finalmente se recogeran de RDR o del fichero de CALYPSO
    val addCollectRegulationDf = addPostRegulationDf.withColumn("cod_collect_regulations", lit("CFTC"))

    val dfpartSIMM = addCollectRegulationDf.filter(col("cod_im_model") <=> "SIMM")
    val dfpartSchedule = addCollectRegulationDf.filter(col("cod_im_model") <=> "Schedule")


    // TODO Informar de boletas sin regla ni excepción

    // Cargamos la tabla donde obtendremos los diferentes tramos de buckets
    info(s"Cargando la tabla de bucketizacion...")
//    val tablaBucketDF = repositoryBucketSimm.loadTable
    val tablaBucketDF = params("repositoryBucketSimmDF")
      .cache
    info("INFO_OCELOTE 113: " + tablaBucketDF.count)

    // Aplicamos bucket sobre el valor imp_amount_sens, posteriormente y solo para DeltaIr obtenemos
    // la bucketizacion sobre el importe imp_amount_cup0, para ello primeros divimos entre el importe imp_amount_sens,
    // y asi obtenemos el factor de bucketización por el que multiplicar
    // Ademas dejamos el product_class correspondiente a cada tipo (SIMM o SCHEDULE)
    val dfaddQplBucketSIMM = calculateBucket(dfpartSIMM, tablaBucketDF)
      .withColumnRenamed("key", "qpl_bucket")
      .withColumn("imp_amount_cup0", when((col("qpl_bucket") <=> "") || (col("des_sourcename") =!= "DeltaIR"),
        col("imp_amount_cup0").cast("decimal(23,8)"))
        .otherwise((col("value").cast("decimal(23,8)") / col("imp_amount_sens").cast("decimal(23,8)"))
          * col("imp_amount_cup0").cast("decimal(23,8)")))
      .withColumn("factor", (col("value")/ col("imp_amount_sens")).cast("decimal(23,8)"))
      .withColumn("imp_amount_sens", when(col("qpl_bucket") <=> "", col("imp_amount_sens").cast("decimal(23,8)"))
        .otherwise(col("value")).cast("decimal(23,8)"))
      .withColumn("imp_pnlvar_deltair", when(col("qpl_bucket") <=> "", col("imp_pnlvar_deltair").cast("decimal(23,8)"))
        .otherwise(col("imp_pnlvar_deltair")*col("factor")).cast("decimal(23,8)"))
      .withColumn("imp_pnlvar_vegair", when(col("qpl_bucket") <=> "", col("imp_pnlvar_vegair").cast("decimal(23,8)"))
        .otherwise(col("imp_pnlvar_vegair")*col("factor")).cast("decimal(23,8)"))
      .drop(col("value"))
      .drop(col("factor"))
      .drop(col("cod_product_class_schedule"))
      .withColumn("fec_vencimiento", lit("")) // Cambiamos la fecha de vencimiento a vacia
      .withColumn("cod_qualifier", when(col("cod_qualifier") === lit("UDI"), lit("MXV")).otherwise(col("cod_qualifier")))
    info("INFO_OCELOTE 114: " + dfaddQplBucketSIMM.count)

    // Creamos el nuevo campo und_maturity lo mas cercano a los tramos de IR
    val dfaddStretchUndMaturity = calculateStretch(dfaddQplBucketSIMM, tablaBucketDF).cache
    info("INFO_OCELOTE 115: " + dfaddStretchUndMaturity.count)

    val dfaddQplBucketSchedule = dfpartSchedule.withColumn("qpl_bucket", lit(""))
      .withColumn("cod_qualifier", concat(col("cod_family"), lit("-"), col("cod_group"), lit("-"), col("cod_type")))
      .drop(col("cod_product_class"))
      .withColumnRenamed("cod_product_class_schedule", "cod_product_class")
    //      .cache
    info("INFO_OCELOTE 116: " + dfaddQplBucketSchedule.count)

    // Llamamos a la función para realizar todos los campos de la volatilidad
    val unionSIMMVol = calculatesVolatility(dfaddStretchUndMaturity, tablaBucketDF, datePa, params)
    tablaBucketDF.unpersist
    info("INFO_OCELOTE 117: " + unionSIMMVol.count)

    val preSelectDF = (unionSIMMVol union dfaddQplBucketSchedule)
      .withColumn("cod_type_simm", lit(appId))
    info("INFO_OCELOTE 118: " + preSelectDF.count)

    // Realizamos un select con los campos que deseamos persistir en tabla
    val simmDF = preSelectDF.select(col("cod_entity"), col("cod_portfolio"), col("cod_nb"), col("cod_match_id"),
      col("fec_tradedate"), col("cod_issuercode"), col("cod_counterparty").as("cod_counterparty_mx"),
      col("cod_shortname"), col("cod_contract_calypso"), col("cod_im_model"), col("cod_product_class"),
      col("cod_post_regulations"), col("cod_collect_regulations"), col("cod_risk_type"), col("cod_qualifier"),
      col("cod_sector_isda"), col("des_riskfactor"), col("fec_sens_maturity"), col("qpl_bucket"),
      col("qpl_undmaturity"), col("cod_label2"), col("cod_curr_amount"), col("des_sourcename"), col("imp_amount_sens"),
      col("imp_amount_cup0"), col("des_sourcesystem"), col("fec_horizondate"), col("fec_alta"), col("fec_vencimiento"),
      col("cod_tipooperacion").as("cod_direccion"), col("cod_skeleton_code"), col("cod_prod_rtce"),
      col("cod_prod_rtce_dom"), col("imp_pnlvar_deltair"), col("imp_pnlvar_vegair"), col("imp_pnlvar_deltafx"),
      col("year"), col("month"), col("day"), col("cod_type_simm"))

    dfaddStretchUndMaturity.unpersist
    productClassDF.unpersist()

    // Persistir en la tabla SIMM
    //    info(s"Guardando la tabla SIMM ...")
//    repositorySimm.saveTableMaster(simmDF, appId)
    info("INFO_OCELOTE 100: " + simmDF.count)
    Map("dos" -> simmDF)

  }

  /**
    * // Añadimos products_class, product_class_schedule y productos rtce dominante para los productos con estrategia
    *
    * @param dfWithStrategy Dataframe de sensibilidades con estrategia tipo ST-%
    * @param productsDF     Dataframe de productos
    * @return Dataframe de sensibilidades con los product_class añadido y productos rtce
    */
  private def calculateProductsWithStrategy(dfWithStrategy: DataFrame, productsDF: DataFrame): DataFrame = {
    val addWeightToSIMM = dfWithStrategy.as("simm").join(productsDF.as("pro"),
      dfWithStrategy("cod_prod_rtce") === productsDF("cod_rtceproductid"))
      .select("simm.cod_prod_rtce", "simm.cod_strategy", "pro.qnu_weight",
        "pro.xti_isim", "pro.xti_credit_risk", "pro.cod_product_class", "pro.cod_product_class_schedule")


    val filterMaxWeight = addWeightToSIMM.select(col("cod_strategy"), struct(col("qnu_weight"),
      col("xti_credit_risk"), col("xti_isim"), col("cod_product_class"), col("cod_product_class_schedule"),
      col("cod_prod_rtce").as("cod_prod_rtce_dom")).as("ord"))
      .groupBy(col("cod_strategy"))
      .agg(max("ord").alias("ord"))
      .select("cod_strategy", "ord.xti_credit_risk", "ord.xti_isim", "ord.cod_product_class",
        "ord.cod_product_class_schedule", "ord.cod_prod_rtce_dom") // $"vs.Category", $"vs.TotalValue")

    val dominantStrategyDF = dfWithStrategy.as("stg").join(filterMaxWeight.as("filtros"),
      dfWithStrategy("cod_strategy") === filterMaxWeight("cod_strategy"))
      //"cod_strategy")
      .filter((col("xti_isim") === lit("Y")) && (col("xti_credit_risk") === lit("Y")))
      .drop(col("xti_isim"))
      .drop(col("xti_credit_risk"))
      .drop(col("filtros.cod_strategy"))

    dominantStrategyDF
  }

  /**
    * // Añadimos products_class, product_class_schedule y productos rtce dominante para los productos SIN estrategia
    *
    * @param dfWithOUTStrategy Dataframe de sensibilidades sin estrategia o con estrategia diferente al tipo ST-%
    * @param productsDF        Dataframe de productos
    * @return Dataframe de sensibilidades con los product_class añadido y productos rtce
    */
  private def calculateProductsWithOUTStrategy(dfWithOUTStrategy: DataFrame, productsDF: DataFrame): DataFrame = {
    // Nos quedamos solo con los que pasan el filtro de Initial Margin
    val filterProductsDF = productsDF.filter(col("xti_isim") === lit("Y"))
      .drop(col("qnu_weight"))
      .drop(col("xti_credit_risk"))
    // Realizamos el cruce del fichero de productos con el dataframe de SIMM que llevamos
    // TODO Habra que cambiar los cruces por producto rtce
    dfWithOUTStrategy.as("sensi")
      .join(filterProductsDF.as("productClass"),
        dfWithOUTStrategy("cod_prod_rtce") === productsDF("cod_rtceproductid"))
      .select("sensi.*", "productClass.cod_product_class", "productClass.cod_product_class_schedule")
      .withColumn("cod_prod_rtce_dom", col("cod_prod_rtce"))
  }

  /**
    * Cálculo de los diferentes tramos de bucketización
    *
    * @param dfPreSimm - Dataframe de sensibilidades sobre el que aplicar la bucketización
    * @param dfBucket  - Dataframe con los días y tramos de diferentes buckets
    * @return
    */
  private def calculateBucket(dfPreSimm: DataFrame, dfBucket: DataFrame): DataFrame = {
    val arrayIr = createArrayFromBucketDataframe(dfBucket, "ir")
    val arrayCred = createArrayFromBucketDataframe(dfBucket, "cred")
    val mapToFindBucket = createMapFromBucket(dfBucket)

    val bucketing: (String, String, java.math.BigDecimal, String) => Map[String, java.math.BigDecimal] =
      (dateToBucket: String, riskType: String, importe: java.math.BigDecimal, horizondate: String) => {
        val formatter = new SimpleDateFormat("dd/MM/yyyy")
        val todayDate = formatter.parse(horizondate)
        riskType match {
          case "Risk_IRCurve" | "Risk_IRVol" | "Risk_FXVol" => returnBuckets(dateToBucket, todayDate,
            arrayIr, mapToFindBucket, importe)
          case "Risk_CreditQ" => returnBuckets(dateToBucket, todayDate, arrayCred, mapToFindBucket, importe)
          case _ => Map("" -> new java.math.BigDecimal("0"))
        }

      }

    val udfBucketing = udf(bucketing)

    val dfPreSimmAs = dfPreSimm.as("presimm")

    val columnas = dfPreSimmAs.columns
      .map(col) :+ explode(udfBucketing(col("fec_sens_maturity"), col("cod_risk_type"), col("imp_amount_sens"),
      col("fec_horizondate")))

    dfPreSimmAs.select(columnas: _*)
  }

  /**
    * Crear un array a partir del df de buckets y el tipo de datos con los que nos queramos quedar,
    * es decir, ahora mismo los tramos de DelaIr y Credito son diferentes
    *
    * @param dfBucket       - Dataframe de tramos
    * @param des_typeFilter - Que tipo de tramos hay que elgir (DeltaIr, Credito)
    * @return - Array con los dias ordenados de la sensibilidad necesitada
    */
  private def createArrayFromBucketDataframe(dfBucket: DataFrame, des_typeFilter: String): Array[Int] = {
    dfBucket.filter(col("des_type") === des_typeFilter)
      .select(col("qnu_interval_days"))
      .collect
      .map(_.getInt(0))
      .sorted
  }

  /**
    * Obtenemos un mapa con los dias como clave y el tramos correspondiente como valor para su futura traducción
    * en el proceso de bucketización
    *
    * @param dfBucket - Dataframe de tramos
    * @return - Map con los dias como clave y el tramos correspondiente como valor
    */
  private def createMapFromBucket(dfBucket: DataFrame): Map[Int, String] = {
    import spark.implicits._
    dfBucket.dropDuplicates(Seq("qnu_interval_days"))
      .select(col("qnu_interval_days"), col("des_interval"))
      .as[(Int, String)]
      .collect
      .toMap
  }

  /**
    * Devolvemos un nuevo dataframe con los tramos del campo undmaturity adaptados a los que aparecen en el array
    * correspodiente
    *
    * @param dfVol    Dataframe a adaptar los tramos
    * @param dfBucket Dataframe con los tramos a los que adaptar
    * @return dataframe con los tramos del campo undmaturity adaptados a los ordenamos por el regulatorio
    */
  private def calculateStretch(dfVol: DataFrame, dfBucket: DataFrame): DataFrame = {
    val arrayIr = createArrayFromBucketDataframe(dfBucket, "ir")
    val mapToFindBucket = createMapFromBucket(dfBucket)

    val getStretch: (String, String) => String =
      (dateToBucket: String, horizondate: String) => {
        val formatter = if (horizondate.contains('/')) new SimpleDateFormat("dd/MM/yyyy")
        else new SimpleDateFormat("yyyyMMdd")
        val todayDate = formatter.parse(horizondate)
        getCloseValueDateToBucket(dateToBucket, todayDate, arrayIr, mapToFindBucket)
      }

    val udfStretch = udf(getStretch)

    dfVol.withColumn("qpl_undmaturity_stretch", when(col("qpl_undmaturity") =!= lit(""),
      udfStretch(col("qpl_undmaturity"), col("fec_horizondate"))).otherwise(lit("")))
  }

  def getCloseValueDateToBucket(dateToBucket: String, destinoDate: Date, arrayDiferentDays: Array[Int],
    mapToFindBucket: Map[Int, String]): String = {
    val dateInDays = parseToDays(dateToBucket, destinoDate)
    val ((weightInf, weightUp), (extremeInf, extremeUp)) = calculatedWeight(dateInDays, arrayDiferentDays)
    if ((extremeInf == 0 || extremeUp == 0) && (extremeInf == 100 || extremeUp == 100)) { //||
      //      weightInf.toString == "1") {
      if (extremeInf == 0 && extremeUp == 100) {
        mapToFindBucket.getOrElse(arrayDiferentDays(arrayDiferentDays.length - 1), "None")
      } else if (extremeInf == 100 && extremeUp == 0) {
        mapToFindBucket.getOrElse(arrayDiferentDays(0), "None")
      } else {
        if (weightInf.compareTo(weightUp) >= 0) mapToFindBucket.getOrElse(extremeInf, "None")
        else mapToFindBucket.getOrElse(extremeUp, "None")
      }
    }
    else {
      if (weightInf.compareTo(weightUp) >= 0) mapToFindBucket.getOrElse(extremeInf, "None")
      else mapToFindBucket.getOrElse(extremeUp, "None")
    }
  }

  /**
    *
    * @param dfaddStretchUndMaturity Desc
    * @param tablaBucketDF Desc
    * @param datePa Desc
    * @return
    */
  private def calculatesVolatility(dfaddStretchUndMaturity: DataFrame, tablaBucketDF: DataFrame,
    datePa: JavaDateParts, params: Map[String, Dataset[Row]]): DataFrame = {
    // Realizamos los filtros necesarios para aplicar la volatilidades a los registros que corresponda
    // Para vegaFX filtro por cod_group='FX'
    val dfSimmVegaFXVol = dfaddStretchUndMaturity.filter(col("cod_group") === lit("FX"))

    // Para vegaIR con undmaturity filtro por sourcename='VegaIR' y el volgroup_agrup='SV'
    val dfSimmVegaIr_SV_Vol_b = dfaddStretchUndMaturity.filter((col("des_sourcename") === lit("VegaIR")) &&
      (col("des_volgroup_agrup") === lit("SV")))
    val dfSimmVegaIr_SV_Vol = add_desVolgroup(dfSimmVegaIr_SV_Vol_b)

    // Para vegaIR sin undmaturity filtro por sourcename='VegaIR' y el volgroup_agrup='CV'
    val dfSimmVegaIr_CV_Vol_b = dfaddStretchUndMaturity.filter((col("des_sourcename") === lit("VegaIR")) &&
      (col("des_volgroup_agrup") === lit("CV")))
    val dfSimmVegaIr_CV_Vol = add_desVolgroup(dfSimmVegaIr_CV_Vol_b)

    // Hacemos otro filtro del fichero con el contrario de los filtros anteriores para luego hacer un union con el resto
    val dfWithOutVol = dfaddStretchUndMaturity.filter((col("cod_group") =!= lit("FX")) &&
      (col("des_volgroup_agrup") =!= lit("CV")) && (col("des_volgroup_agrup") =!= lit("SV")))
      .drop(col("qpl_undmaturity_stretch"))

    // Cargamos las tablas de volatilidades para multiplicarlas por los importes de las sensibilidades
    // Cargamos la tabla de volatilidades FX y multiplicamos empleando los tramos
    info(s"Cargando la partición del dia de la tabla de volatilidades fx...")
//    val loadTableVolFx = repositoryMasterVolfx.loadTable(datePa.year, datePa.month, datePa.day)
    val loadTableVolFx = params("repositoryMasterVolfxDF").where(s"year=${datePa.year} AND month=${datePa.month} AND day=${datePa.day}")
    val bucketVolFx = calculateBucketVol(loadTableVolFx, tablaBucketDF)
      .drop(col("qpl_maturity"))
      .drop(col("imp_volatility"))
      .select(col("cod_currpair"), col("key").as("qpl_maturity"), col("value").as("imp_volatility"), col("des_voltype"))

    // TODO CAMPOS incorrectos de murex: el cod_group debe ser FX
    // Realizar Join, cod_group y multiplicamos por la volatilidad
    val dfJoinVolVegaFX = dfSimmVegaFXVol.as("vegafx").join(bucketVolFx.as("volfx"),
      dfSimmVegaFXVol("cod_group") === bucketVolFx("des_voltype") &&
        dfSimmVegaFXVol("cod_currpair") === bucketVolFx("cod_currpair") &&
        dfSimmVegaFXVol("qpl_bucket") === bucketVolFx("qpl_maturity"))
      .select("vegafx.*", "volfx.imp_volatility")
      .withColumn("imp_amount_sens",
        col("imp_amount_sens").cast("decimal(23,8)") * col("imp_volatility").cast("decimal(23,8)"))
      .drop(col("imp_volatility"))
      .drop(col("qpl_undmaturity_stretch"))

    val calculateDaysFromMultiplicityCode: String => Int = (s: String) => {
      if (s.contains('Y') && s.contains('M') && s.length >= 4) {
        val arrayYear = s.split("Y")
        val yearsSub = arrayYear(0).toInt
        val monthSub = arrayYear(1).split("M")(0).toInt
        (yearsSub * 365) + (monthSub * 30)
      }
      else {
        val code = s.last.toString
        val periodo = s.init.toInt
        code match {
          case "D" => 1 * periodo
          case "W" => 7 * periodo
          case "M" => 30 * periodo
          case "Y" => 365 * periodo
        }
      }
    }
    val calculateDaysFromMultiplicityCodeudf = udf(calculateDaysFromMultiplicityCode)

    //Cargamos las tabla de volatilidades de vegair sin undmaturity
    //ademas cambiamos posibles "12M" por "1Y"  y pasamos buckets a dias

    info(s"Cargando la partición del dia de la tabla de volatilidades cf...")
//    val loadTableVolVegairCF = repositoryMasterVolcf.loadTable(datePa.year, datePa.month, datePa.day)
    val loadTableVolVegairCF = params("repositoryMasterVolcfDF").where(s"year=${datePa.year} AND month=${datePa.month} AND day=${datePa.day}")
    // Cambiamos 12M por 1Y en qpl_maturity. añadimos columna qpl_maturity_12Mto1Y
    val loadTableVolVegairCF_12Mto1Y =loadTableVolVegairCF.withColumn("qpl_maturity_12Mto1Y",
      when(col("qpl_maturity")<=>lit("12M"),"1Y").otherwise(col("qpl_maturity")))
    //añadimos los buckets en dias
    val loadTableVolVegairCF_12Mto1Y_days= loadTableVolVegairCF_12Mto1Y.withColumn("qpl_maturity_days",
      calculateDaysFromMultiplicityCodeudf(col("qpl_maturity_12Mto1Y")))

    // A continuación. 1.- identificamos los buckets de los que no tenemos imp_volatility (join left outer y
    // nos quedamos con nulls. 2.- unimos a la tabla de vegair los buckets que faltan y aprox con imp_volatility más
    // cernana.  3.- hacemos join para multiplicar por imp_volatility ahora que ya tenemos todas.

    // 1.- identificamos los nulls
    val dfSimmVegaIr_CV_Vol_null = dfSimmVegaIr_CV_Vol.as("vegairCF").join(loadTableVolVegairCF_12Mto1Y.as("volCF"),
      dfSimmVegaIr_CV_Vol("des_volgroup") === loadTableVolVegairCF_12Mto1Y("cod_currency") &&
        dfSimmVegaIr_CV_Vol("qpl_bucket") === loadTableVolVegairCF_12Mto1Y("qpl_maturity_12Mto1Y"),"left_outer")
      .select("vegairCF.des_volgroup","vegairCF.qpl_bucket","volCF.imp_volatility")
      .filter(col("imp_volatility").isNull)
      .withColumn("qpl_maturity_days",calculateDaysFromMultiplicityCodeudf(col("qpl_bucket")))
      //.drop("qpl_bucket")
      .withColumnRenamed("des_volgroup","cod_currency")
      .dropDuplicates(Seq("cod_currency","qpl_maturity_days"))

    // 2.- unimos los buckets en los que no tenemos imp_volatility para aproximarla.
    val loadTableVolVegairCF_12Mto1Y_union = loadTableVolVegairCF_12Mto1Y_days.
      select("cod_currency","qpl_maturity_12Mto1Y","imp_volatility","qpl_maturity_days")
      .union(dfSimmVegaIr_CV_Vol_null)

    // 2.1 aproximamos al dato anterior o posterior de imp_volatility.
    val window = Window.partitionBy("cod_currency").orderBy("cod_currency","qpl_maturity_days")
    def vegairextended(df:DataFrame): DataFrame={

      df
        .withColumn("imp_volatility_extended",
          when(col("imp_volatility").isNull && lag("imp_volatility", 1).over(window).isNull,
            lead("imp_volatility", 1, null).over(window))
            .otherwise(when(col("imp_volatility").isNull && lag("imp_volatility", 1).over(window).isNotNull,
              lag("imp_volatility", 1, null).over(window))
              .otherwise(col("imp_volatility"))))
        .drop("imp_volatility")
        .withColumnRenamed("imp_volatility_extended", "imp_volatility")
    }
    def bigvegairextended(df:DataFrame): DataFrame= {
      //if(df.filter(col("imp_volatility").isNull).limit(1).collect().isEmpty == true){df}
      if(df.filter(col("imp_volatility").isNull).count==0){df}
      else {bigvegairextended(vegairextended(df)) }
    }
    // Obtenemos la nueva tabla de vegaIR incluyendo aquellos buckets que faltaban
    val loadTableVolVegairCF_12Mto1Y_extended = bigvegairextended(loadTableVolVegairCF_12Mto1Y_union)

    // 3  hacemos join con la tabla de volatilidades ya extendida con las imp_volatility que faltan.
    val dfJoinVolVegairCF = dfSimmVegaIr_CV_Vol.as("vegairCF")
      .join(loadTableVolVegairCF_12Mto1Y_extended.as("volCF"),
        dfSimmVegaIr_CV_Vol("des_volgroup") === loadTableVolVegairCF_12Mto1Y_extended("cod_currency") &&
          dfSimmVegaIr_CV_Vol("qpl_bucket") === loadTableVolVegairCF_12Mto1Y_extended("qpl_maturity_12Mto1Y")
      ).select("vegairCF.*","volCF.imp_volatility")
      .withColumn("imp_amount_sens",
        col("imp_amount_sens").cast("decimal(23,8)") * col("imp_volatility").cast("decimal(23,8)"))
      .withColumn("qpl_undmaturity", lit("N/A"))
      .drop(col("imp_volatility"))
      .drop(col("qpl_undmaturity_stretch"))
      .drop(col("des_volgroup"))

    info(s"Carga de la partición del dia de la tabla de volatilidades cf...  HECHA")

    //Cargamos las tabla de volatilidades de vegair con undmaturity
    info(s"Cargando la partición del dia de la tabla de volatilidades sw...")
//    val loadTableVolVegairSW = repositoryMasterVolsw.loadTable(datePa.year, datePa.month, datePa.day)
    val loadTableVolVegairSW = params("repositoryMasterVolswDF").where(s"year=${datePa.year} AND month=${datePa.month} AND day=${datePa.day}")
    // Cambiamos 12M por 1Y en qpl_maturity. añadimos columna qpl_maturity_12Mto1Y
    val loadTableVolVegairSW_12Mto1Y =loadTableVolVegairSW.withColumn("qpl_maturity_12Mto1Y",
      when(col("qpl_maturity")<=>lit("12M"),"1Y").otherwise(col("qpl_maturity")))
    //añadimos los buckets en dias
    val loadTableVolVegairSW_12Mto1Y_days= loadTableVolVegairSW_12Mto1Y.withColumn("qpl_undmaturity_days",
      calculateDaysFromMultiplicityCodeudf(col("qpl_undmaturity")))
      .withColumn("qpl_maturity_days",
        calculateDaysFromMultiplicityCodeudf(col("qpl_maturity")))
    // A continuación. 1.- identificamos los buckets de los que no tenemos imp_volatility (join left outer y
    // nos quedamos con nulls. 2.- unimos a la tabla de vegair los buckets que faltan y aprox con imp_volatility más
    // cernana.  3.- hacemos join para multiplicar por imp_volatility ahora que ya tenemos todas.

    // 1.- identificamos los nulls
    val dfSimmVegaIr_SV_Vol_null = dfSimmVegaIr_SV_Vol.as("vegairCF1").join(loadTableVolVegairSW_12Mto1Y.as("volCF1"),
      dfSimmVegaIr_SV_Vol("des_volgroup") === loadTableVolVegairSW_12Mto1Y("cod_currency") &&
        dfSimmVegaIr_SV_Vol("qpl_bucket") === loadTableVolVegairSW_12Mto1Y("qpl_maturity_12Mto1Y") &&
        dfSimmVegaIr_SV_Vol("qpl_undmaturity_stretch") === loadTableVolVegairSW_12Mto1Y("qpl_undmaturity"),"left_outer")
      .select("vegairCF1.des_volgroup","vegairCF1.qpl_bucket","vegairCF1.qpl_undmaturity_stretch","volCF1.imp_volatility")
      .filter(col("imp_volatility").isNull)
      .withColumn("qpl_undmaturity_days",calculateDaysFromMultiplicityCodeudf(col("qpl_undmaturity_stretch")))
      .withColumn("qpl_maturity_days",
        calculateDaysFromMultiplicityCodeudf(col("qpl_bucket")))
      //.drop("qpl_bucket")
      .withColumnRenamed("des_volgroup","cod_currency")
      .dropDuplicates(Seq("cod_currency","qpl_undmaturity_days","qpl_maturity_days"))

    // 2.- unimos los buckets en los que no tenemos imp_volatility para aproximarla.
    val loadTableVolVegairSW_12Mto1Y_union = loadTableVolVegairSW_12Mto1Y_days.
      select("cod_currency","qpl_maturity_12Mto1Y","qpl_undmaturity","imp_volatility","qpl_undmaturity_days",
        "qpl_maturity_days")
      .union(dfSimmVegaIr_SV_Vol_null)
    // 2.1 aproximamos al dato anterior o posterior de imp_volatility.
    val window_SW = Window.partitionBy("cod_currency","qpl_maturity_days")
      .orderBy("cod_currency","qpl_maturity_days","qpl_undmaturity_days")

    // Obtenemos la nueva tabla de vegaIR incluyendo aquellos buckets que faltaban
    val loadTableVolVegairSW_12Mto1Y_extended = bigvegairextended(loadTableVolVegairSW_12Mto1Y_union)

    //3  hacemos join con la tabla de volatilidades ya extendida con las imp_volatility que faltan
    val dfJoinVolVegairSW = dfSimmVegaIr_SV_Vol.as("vegairCV").join(loadTableVolVegairSW_12Mto1Y_extended.as("volCV"),
      dfSimmVegaIr_SV_Vol("des_volgroup") === loadTableVolVegairSW_12Mto1Y_extended("cod_currency") &&
        dfSimmVegaIr_SV_Vol("qpl_bucket") === loadTableVolVegairSW_12Mto1Y_extended("qpl_maturity_12Mto1Y") &&
        dfSimmVegaIr_SV_Vol("qpl_undmaturity_stretch") === loadTableVolVegairSW_12Mto1Y_extended("qpl_undmaturity")
    ).select("vegairCV.*", "volCV.imp_volatility")
      .withColumn("imp_amount_sens",
        col("imp_amount_sens").cast("decimal(23,8)") * col("imp_volatility").cast("decimal(23,8)"))
      .drop(col("imp_volatility"))
      .drop(col("qpl_undmaturity_stretch"))
      .drop(col("des_volgroup"))

    dfJoinVolVegaFX union dfJoinVolVegairCF union dfJoinVolVegairSW union dfWithOutVol
  }

  private def add_desVolgroup(df: DataFrame): DataFrame = {
    // Tenemos que cruzar por la moneda la cual obtenemos de los tres primeros caracteres del volgroup,
    // pero la tenemos nombrada riskfactor, ademas tenemos un par de excepciones
    df.withColumn("des_volgroup",
      when(col("des_riskfactor").isin("EXOT_IRPH_1", "EXOT_IRPH_2", "EXOT_IRPH_3", "SPIPC"), lit("EUR"))
        .otherwise(when((col("des_riskfactor") like "TIIE%")||(col("des_riskfactor") like "MX SWAPT") , lit("MXN"))
          .otherwise(substring(col("des_riskfactor"), 0, 3))))
  }

  private def calculateBucketVol(dfVol: DataFrame, dfBucket: DataFrame): DataFrame = {
    val arrayIr = createArrayFromBucketDataframe(dfBucket, "ir")
    val mapToFindBucket = createMapFromBucket(dfBucket)

    val bucketing: (String, java.math.BigDecimal, String) => Map[String, java.math.BigDecimal] =
      (dateToBucket: String, importe: java.math.BigDecimal, horizondate: String) => {
        val formatter = new SimpleDateFormat("yyyyMMdd")
        val todayDate = formatter.parse(horizondate)
        returnBuckets(dateToBucket, todayDate, arrayIr, mapToFindBucket, importe)
      }

    val udfBucketing = udf(bucketing)

    val columnas = dfVol.columns
      .map(col) :+ explode(udfBucketing(col("qpl_maturity"), col("imp_volatility"),
      col("fec_horizondate")))

    dfVol.select(columnas: _*)
  }

  /**
    * Método para devolver los tramos de bucketización
    *
    * @param dateToBucket desc
    * @param destinoDate desc
    * @param arrayDiferentDays desc
    * @param mapToFindBucket desc
    * @param importe desc
    * @return
    */
  def returnBuckets(dateToBucket: String, destinoDate: Date, arrayDiferentDays: Array[Int],
    mapToFindBucket: Map[Int, String], importe: java.math.BigDecimal): Map[String, java.math.BigDecimal] = {
    val dateInDays = parseToDays(dateToBucket, destinoDate)
    val ((weightInf, weightUp), (extremeInf, extremeUp)) = calculatedWeight(dateInDays, arrayDiferentDays)
    val pillarUpper = mapToFindBucket.get(extremeUp)
    val impUpper = weightUp.multiply(importe)

    val pillarInf = mapToFindBucket.get(extremeInf)
    val impInf = weightInf.multiply(importe)
    if ((extremeInf == 0 || extremeUp == 0) && (extremeInf == 100 || extremeUp == 100) ||
      weightInf.toString == "1") {
      // Esta en el punto
      if (extremeInf == 0 && extremeUp == 100) {
        val pillar = mapToFindBucket(arrayDiferentDays(arrayDiferentDays.length - 1))
        Map(pillar -> importe)
      } else if (extremeInf == 100 && extremeUp == 0) {
        val pillar = mapToFindBucket(arrayDiferentDays(0))
        Map(pillar -> importe)
      } else {
        Map(pillarInf.get -> impInf)
      }
    }
    else {
      Map(pillarInf.get -> impInf, pillarUpper.get -> impUpper)
    }
  }

  /**
    * Metodo que evalua cada linea del dataframe de contrapartidas para obtener de cada xml la contrapartida de rdr
    * y su correspondiente contrapartida en Murex 3.
    * @param contrapartidasDF - Dataframe de contrapartidas fomado por xml
    * @return Dataframe con las columnas de contrapartidas Murex 3 o Calypso, la de contrpartida rdr y el identificador
    *         (murex o calypso) añadidas
    */
  private def addContrapartidaFromRdr(contrapartidasDF: DataFrame): DataFrame = { // TODO OK SIN ACCESOS EXTERNOS

    val getContrapartidaRDR: String => Seq[(String, String, String)] = (xmlContrapartidas: String)  =>{
      val xmlContrapartida = xml.XML.loadString(xmlContrapartidas)
      val resultSeq = for (a <- xmlContrapartida \\ "OPERATIVE";
                           b <- a \\ "ROLE_IDENTIFIER";
                           c <- b \\ "Role_Identifier_Context" if c.text.equals("MUREXID") || c.text.equals("CALYPSOID")
      ) yield ((b \\ "Role_Identifier").text,(a \\ "RDR_Code_Operative").text, c.text)

      resultSeq
    }

    val udfAddCounterParty = udf(getContrapartidaRDR)

    // Al realizar el explode de un tipo Seq, me crea tantas columnas nuevas, llamadas _1, _2 y sucesivamente con cada
    // uno de los valores de la Seq. Renombramos para adaptarlo al map anterior
    contrapartidasDF.withColumn("completeSequence", explode(udfAddCounterParty(col("des_xml"))))
      .select("completeSequence.*")
      .select(col("_1").as("key"), col("_2").as("value"), col("_3").as("origenId"))
  }

  /**
    * Realiza un join entre la tabla de todas las contrapartidas con formato calypso y el
    * fichero de calypso que nos informa del perimetro de
    * contrapartidas, para realizar un filtro y quedarnos solo con las contrapartidas del perimetro, con sus codigo de
    * entidad correspondiente
    * @param dfRDR - Dataframe de todas las contrapartidas de RDR
    * @param dfCalypso - Dataframe de calypso que nos informa del perimetro de contrapartidas
    * @return Dataframe resultante del inner join entre ambos
    */
  private def contrapartidaFiltro(dfRDR: DataFrame, dfCalypso: DataFrame): DataFrame = { // TODO OK SIN ACCESOS EXTERNOS
    dfRDR.as("rdr").join(
      dfCalypso.as("calypso"),
      col("rdr.key") <=> col("calypso.cod_shortname"))
      .select("rdr.*", "calypso.cod_entity", "calypso.cod_contract_calypso", "calypso.fec_effectivedate")
  }

  /**
    * Realizamos un join entre todas las contrapartidas de RDR y Dataframe con las contrapartidas filtradas para obtener
    * el filtrado con las contrapartidas de murex y RDR. Además, aunque no deberia, borramos los posibles duplicados de
    * contrapartidas de Murex
    * @param dfRDR Dataframe de todas las contrapartidas de RDR
    * @param dfFiltradoCalypso Dataframe con las contrapartidas filtradas
    * @return Dataframe filtrado con la contrapartida de murex y RDR
    */
  private def recuperarContrapartidasMx(dfRDR: DataFrame, dfFiltradoCalypso: DataFrame): DataFrame = { // TODO OK SIN ACCESOS EXTERNOS
    dfRDR.as("rdr").join(
      dfFiltradoCalypso.as("filtrado"),
      col("rdr.value") <=> col("filtrado.value"))
      .select("rdr.*", "filtrado.cod_entity", "filtrado.cod_contract_calypso", "filtrado.fec_effectivedate")
      .filter(col("origenId") === "MUREXID")
      .dropDuplicates(Seq("key"))
  }

}


object SIMMLoadApp {
  val design = BatchBaseApp.design + SparkService.design
}
object Main {
  def main(args: Array[String]): Unit = {
    val design = SIMMLoadApp.design + newDesign.bind[Array[String]].toInstance(args)
    design.build[SIMMLoadApp] { app: SIMMLoadApp => app.exec() }
  }
}
