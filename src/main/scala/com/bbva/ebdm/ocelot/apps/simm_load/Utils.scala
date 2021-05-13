package com.bbva.ebdm.ocelot.apps.simm_load


import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.joda.time.{DateTime, Duration}
import wvlet.log.LogSupport
import scala.util.{Failure, Success, Try}

//sealed trait InputDataType {
//  def getDF(): Dataset[Row]
//}
sealed trait OutnputDataType{
//  def saveData(): Unit = ???
}

trait Utils extends LogSupport {

  // ***********************  COMMON  ***********************

//  case class StringToHDFS(path: String, content: String) extends OutnputDataType // NEW con el   protected def create(path: String, content: String) {
  case class SaveDataframeWithAsveTableMaster(df: DataFrame) extends OutnputDataType // NEW con el def saveTableMasterOLD(dfToWrite: DataFrame): Unit = {
  case class OutputRequest(idType: String, id: Int, date: Date) extends  OutnputDataType//  gsr


  case class JavaDateParts(year: Int, month: Int, day: Int)

//  protected val defaultFileNameTimestampFormat = "yyyyMMdd_HHmmss"
//  protected val FEC_PROCESS = "fec_process"
//  protected val defaultDateFormat = "yyyyMMdd"
//  protected val generationRequestFlagName = "output_generation_request.flg"
  protected val timeFormat = "yyyy-MM-dd HH:mm:ss:SSS"
//  protected val COMA = "\""

  protected val DefaulPartitions = 8

  //  private def create(path: String, content: Array[Byte]) {
  //    val os = dfs.create(new Path("?????" + path))
  //    os.write(content)
  //    os.hsync()
  //  }

  //  protected def create(path: String, content: String) {
  //    create(path, content.getBytes)
  //  }

  // IMPLEMENTED
  def isEmptyDF(df: DataFrame) : Boolean = {
    df.rdd.isEmpty
  }

  // Convierte una fecha en formato dd/MM/yyyy a yyyyMMdd, ambos de tipo string.
  def dateHorizonDateToString(horizonDate: String): String = {
    horizonDate.substring(6) + horizonDate.substring(3,5) + horizonDate.substring(0,2)
  }

  // Convierte una fecha en formato dd/MM/yyyy o dd-MM-YYYY a YYYY-MM-dd
  def spanishDateToAmericanDate(spanishDateString: String): String = {
    val partitionYear = spanishDateString.substring(6)
    val partitionMonth = spanishDateString.substring(3,5)
    val partitionDay = spanishDateString.substring(0, 2)

    val horizonDate = partitionYear + "-" + partitionMonth + "-" + partitionDay
    horizonDate
  }

  def loadMaxPartition(df: DataFrame, yearMonthAndDay: Option[Array[Int]]): DataFrame = {
    yearMonthAndDay match {
      case Some(yearMonthAndDayArray) if yearMonthAndDayArray.length == 3 =>
        df.where(s"year=${yearMonthAndDayArray(0)} AND month=${yearMonthAndDayArray(1)} AND day=${yearMonthAndDayArray(2)}")
      case _ => df
    }
  }

//  def maxPartition(fullTableName: String):Array[Int] = {
//    val expr = (id: Int, colName: String) => regexp_extract(col("result"), "year=(\\d{4})/month=(\\d{1,2})/day=(\\d{1,2}).*", id).as(colName).cast(IntegerType)
//
//    val dfPart = spark.sql(s"show partitions $fullTableName")
//      .select(expr(1, "year"), expr(2, "month"), expr(3, "day"))
//      .orderBy(col("year").desc, col("month").desc, col("day").desc)
//    val firstRow = dfPart.first()
//    Array(firstRow.getInt(0),firstRow.getInt(1),firstRow.getInt(2))
//  }

  //  def formatDate(date: LocalDate, format: String): String = {
  //    date.toString(format)
  //  }

  //  def formatDate(date: LocalDateTime, format: String): String = {
  //    date.toString(format)
  //  }

  def formatDate(date: Date, format: String): String = {
    val sdf = new SimpleDateFormat(format)
    sdf.format(date)
  }

  private def isInternationalHoliday(day: Int, month: Int): Boolean = {
    (day, month) match {
      case (1, 1) | (1, 5) | (25, 12) => true
      case _ => false
    }
  }

  private def isNonWorking(date: Date): Boolean = {
    val cal = getCalendarFromDate(date)
    val dayOfWeek = cal.get(Calendar.DAY_OF_WEEK)
    val dayOfMonth = cal.get(Calendar.DAY_OF_MONTH)
    val month = cal.get(Calendar.MONTH) + 1 // el mes empieza en 0...
    dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY ||
      isInternationalHoliday(dayOfMonth, month)
  }

  private def isBeforeTime(date: Date, time: String): Boolean = {
    formatDate(date, "HHmmss") < time
  }

  def parseDate(text: String, format: String): Date = {
    val sdf = new SimpleDateFormat(format)
    sdf.parse(text)
  }

  private def setTime(date: Date, time: String): Date = {
    val dateStr = formatDate(date, "yyyyMMdd")
    parseDate(dateStr + time, "yyyyMMddHHmmss")
  }

  private object JavaDatePart extends Enumeration {
    val Year, Month, Day, Hour, Minute, Second, Millisecond = Value
  }

  private def javaDatePartToCalendarField(javaDatePart: JavaDatePart.Value): Int = javaDatePart match {
    case JavaDatePart.Year => Calendar.YEAR
    case JavaDatePart.Month => Calendar.MONTH
    case JavaDatePart.Day => Calendar.DAY_OF_MONTH
    case JavaDatePart.Hour => Calendar.HOUR_OF_DAY
    case JavaDatePart.Minute => Calendar.MINUTE
    case JavaDatePart.Second => Calendar.SECOND
    case JavaDatePart.Millisecond => Calendar.MILLISECOND
  }

  private def getRelativeDate(date: Date, datePart: JavaDatePart.Value, differenceAmount: Int): Date = {
    val cal = getCalendarFromDate(date)
    cal.add(javaDatePartToCalendarField(datePart), differenceAmount)
    cal.getTime
  }

  private def getWorkingPeriodStart(date: Date, periodChangeTime: String): Date = {
    if (isNonWorking(date) || isBeforeTime(date, periodChangeTime)) {
      // Si no es día laborable, o si la hora es anterior a la de cambio de periodo
      // se prueba con el día anterior, a la hora de cambio de periodo
      val previousDay = setTime(getRelativeDate(date, JavaDatePart.Day, -1), periodChangeTime)
      getWorkingPeriodStart(previousDay, periodChangeTime)
    } else {
      // Si es un día laborable y la hora es posterior al cambio de periodo,
      // el periodo comienza el mismo día, a la hora de cambio de periodo
      setTime(date, periodChangeTime)
    }
  }

  private def isAfterTime(date: Date, time: String, includeTime: Boolean = true): Boolean = if (includeTime) {
    formatDate(date, "HHmmss") >= time
  } else {
    formatDate(date, "HHmmss") > time
  }

  private def getWorkingPeriodEnd(date: Date, periodChangeTime: String, excludeExactTime: Boolean = true): Date = {
    if (isNonWorking(date) || isAfterTime(date, periodChangeTime, excludeExactTime)) {
      // Si no es día laborable, o si la hora es posterior a la de cambio de periodo
      // se prueba con el día siguiente, a la hora de cambio de periodo
      val nextDay = setTime(getRelativeDate(date, JavaDatePart.Day, 1), periodChangeTime)
      getWorkingPeriodEnd(nextDay, periodChangeTime, excludeExactTime = false)
    } else {
      // Si es un día laborable y la hora es anterior al cambio de periodo,
      // el periodo termina el mismo día, a la hora de cambio de periodo
      setTime(date, periodChangeTime)
    }
  }

  def replaceDates(masks: Seq[String], startDateStr: String, endDateStr: String): Seq[String] = {
    masks.map(mask => mask.replace("{FI}", startDateStr).replace("{FF}", endDateStr))
  }

  def getWorkingPeriod(date: Date, periodChangeTime: String): (Date, Date) = {
    (getWorkingPeriodStart(date, periodChangeTime), getWorkingPeriodEnd(date, periodChangeTime))
  }

  private def getCalendarFromDate(date: Date): Calendar = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal
  }

  def getDateParts(date: Date): JavaDateParts = {
    val cal = getCalendarFromDate(date)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1 // el mes empieza en 0...
    val day = cal.get(Calendar.DAY_OF_MONTH)
    JavaDateParts(year, month, day)
  }

  private def unitToDays(unit: String): Int = unit match {
    case "d" | "D" => 1
    case "w" | "W" | "s" | "S" => 7
    case "m" | "M" => 30
    case "y" | "Y" | "a" | "A" => 365
    case _ => 0
  }

  private def pillarToDays(pillar: String): Int = {
    val NumberUnitPattern = "(\\d+)([dwsmyaDWSMYA])".r
    val UnitNumberPattern = "([dwsmyaDWSMYA])(\\d+)".r

    val (unit, numberStr) = pillar match {
      case NumberUnitPattern(nStr, u) =>
        (u, nStr)
      case UnitNumberPattern(u, nStr) =>
        (u, nStr)
    }
    val number = numberStr.toInt
    unitToDays(unit) * number
  }

  def parseToDays(maturity: String, planDate: Date): Int = {
    //            val optmaturityDate = new SimpleDateFormat("dd MMM yy").parse("20 " + optmaturity)
    val horizondateDate = new DateTime(planDate)
    //val diffDays = Math.abs(new Duration(new DateTime(optmaturityDate), new DateTime(horizondateDate)).getStandardDays)
    if (maturity.length == 6) {
      val maturityDate = new SimpleDateFormat("dd MMM yy").parse("01 " + maturity)
      val diffDays = Math.abs(new Duration(new DateTime(maturityDate), horizondateDate).getStandardDays.toInt)
      diffDays
    } else if (maturity.contains('/')) {
      val maturityDate = new SimpleDateFormat("dd/MM/yyyy").parse(maturity)
      val diffDays = Math.abs(new Duration(new DateTime(maturityDate), horizondateDate).getStandardDays.toInt)
      diffDays
    } else if (maturity.length == 2 || maturity.length == 3) {
      pillarToDays(maturity)
    } else {
      -1
    }
  }

  private def getExtremes(maturityinDays: Int, arrayMaturity: Array[Int]): (Int, Int) = {

    // Este valor es el que devolveremos si es mayor o igual al último valor del array
    var tuple = (0, 100)
    if (maturityinDays >= arrayMaturity(arrayMaturity.length - 1)) {
      tuple = (0, 100)
    } else if (maturityinDays <= arrayMaturity(0)) {
      tuple = (100, 0)
    } else {
      var i = 0
      var keyFound = false
      while (i < arrayMaturity.length && !keyFound) {
        if (maturityinDays >= arrayMaturity(i) && maturityinDays < arrayMaturity(i + 1)) {
          keyFound = true
          tuple = (arrayMaturity(i), arrayMaturity(i + 1))
        }
        i = i + 1
      }
    }
    tuple
  }

  private def getWeight(originalPoint: Int, pointUpper: Int, pointLower: Int): (java.math.BigDecimal, java.math.BigDecimal) = {
    val t0 = originalPoint
    val t1 = pointLower
    val t2 = pointUpper

    val numerador1 = Math.abs(t2-t0).toFloat
    val denominador1 = Math.abs(t2-t1).toFloat
    val weightInf = new java.math.BigDecimal(numerador1/denominador1)

    val numerador2 = Math.abs(t0-t1).toFloat
    val denominador2 = Math.abs(t2-t1).toFloat
    val weightUp = new java.math.BigDecimal(numerador2/denominador2)

    (weightInf, weightUp)
  }

  def calculatedWeight(maturityinDays: Int, arrayMaturity: Array[Int]): ((java.math.BigDecimal, java.math.BigDecimal),(Int,Int)) = {

    var tupleWeight = (new java.math.BigDecimal("0"), new java.math.BigDecimal("0"))
    val extremePoints = getExtremes(maturityinDays, arrayMaturity)
    //var extremePointToReturn = extremePoints
    // si tengo puntos fuera de los extremos devuelvo los pesos a 1 y 0 y el maturity al que corresponden
    if ((extremePoints._1 == 0 || extremePoints._2 == 0) && (extremePoints._1 == 100 || extremePoints._2 == 100)) {
      if (extremePoints._1 == 0 && extremePoints._2 == 100) {
        tupleWeight = (new java.math.BigDecimal("0"), new java.math.BigDecimal("1"))
        //extremePointToReturn = (arrayMaturity(arrayMaturity.length-1), arrayMaturity(arrayMaturity.length-1))
      } else {
        tupleWeight = (new java.math.BigDecimal("1"), new java.math.BigDecimal("0"))
        //extremePointToReturn = (arrayMaturity(0), arrayMaturity(0))
      }
    } else {
      tupleWeight = getWeight(maturityinDays, extremePoints._2, extremePoints._1)
    }
    val salida = (tupleWeight, extremePoints)
    salida
  }


  private def concat(masks: Seq[String]): String = {
    "(?:" + masks.mkString(")|(?:") + ")"
  }

  def filterByFieldMask(dataFrame: DataFrame, masks: Seq[String]): DataFrame = {//"des_path"
    dataFrame.filter(col("des_path").rlike(concat(masks)))
  }

  def filterByFieldMask(dataFrame: DataFrame, masks: String): DataFrame = {//"des_path"
    dataFrame.filter(col("des_path").rlike(masks))
  }

  def everyMaskExists(dataFrame: DataFrame, masks: Seq[String]): Boolean = {
    masks.forall(mask => !isEmptyDF(filterByFieldMask(dataFrame, mask)))

  }

  private def filterByRawLoadDateAfter(dataFrame: DataFrame, rawLoadDate: String): DataFrame = {
    dataFrame.filter(col("fec_raw_load_date") > rawLoadDate)
  }

  private def findMostRecentRawLoadDateByAppId(dataFrame: DataFrame, appId: String): String = {
    dataFrame.filter(col("des_app_id") === appId).orderBy(desc("fec_raw_load_date")).first.getAs[String]("fec_raw_load_date")
  }

  def filterNewFilesForApp(dataFrame: DataFrame, appId: String): DataFrame = {
    // Busca el último registro de la app, para quedarse con los archivos posteriores
    Try(findMostRecentRawLoadDateByAppId(dataFrame, appId)) match {
      case Success(date) =>
        // Si lo encuentra, recupera los posteriores
        filterByRawLoadDateAfter(dataFrame, date)
      case Failure(_) =>
        // Si no lo encuentra, devuelve todos
        dataFrame
    }
  }

//  private def buildGroupRequests(groupIds: Seq[Int], date: Date): Seq[OutputRequest] = {
//    groupIds.map(groupId => OutputRequest("g", groupId, date))
//  }

//  def generateGroupRequestsNEW(groupIds: Seq[Int], date: Date, generationRequestFilesPath: String = ""):  Map[HiveOutput, DataFrame]  = {
//    // TODO ESCRITURA DIFERENTE DE HIVE, DE MOMENTO PARADA
////    val outputRequests = buildGroupRequests(groupIds, date)
////    assert(outputRequests.nonEmpty, "El listado de peticiones no puede estar vacío")
////    val requestDateStr = formatDate(new Date(), defaultFileNameTimestampFormat)
////    val contentToSave = for (outputRequest <- outputRequests) yield {
////      val idType = outputRequest.idType
////      assert(List("g", "o").contains(idType), s"Recibido tipo incorrecto: $idType")
////      val outputDateStr = formatDate(outputRequest.date, defaultDateFormat)
////      val idStr = outputRequest.id.toString
////
////      val requestFileName = s"gesces_request_${idType}_${idStr}_${outputDateStr}_${requestDateStr}.cfg"
////      val requestContent =
////        s"""id_type = $idType
////           |id = $idStr
////           |output_date = $outputDateStr""".stripMargin
////
////      HdfsOutput(generationRequestFilesPath + requestFileName, requestContent) // NEW
////    }
////    contentToSave :+ HdfsOutput(generationRequestFilesPath + generationRequestFlagName, "") // NEW
//    Map() // Since SimmLoadApp is going to remove its hdfs write, this is no longer necessary
//  }

//  def saveFileLoadsDFForApp(dataFrame: DataFrame, appId: String): Map[String, DataFrame] = {
  def saveFileLoadsDFForApp(dataFrame: DataFrame, appId: String): DataFrame = {
    val dateStr = formatDate(new Date(), timeFormat)
    val datePartition = formatDate(new Date(), "yyyyMMdd")
    val df = dataFrame.withColumn("des_app_id", lit(appId))
      .withColumn("fec_created_date", lit(dateStr))
      .withColumn("fec_process", lit(datePartition))
    // There is a "partitionBy(FEC_PROCESS)" which should not be necessary - therefore it is not being applied
//    Map("uno" -> df)
    df
  }

  def saveTableMaster(dfToWrite: DataFrame): OutnputDataType = {
    SaveDataframeWithAsveTableMaster(dfToWrite)
  }

}
