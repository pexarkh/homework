package panalgo

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Date
import java.time.Period
import scala.io.Source

object Panalgo {
  val spark = SparkSession.builder.appName("Panalgo").master("local[*]").getOrCreate

  val PATIENT_DATE_FIELD = "CLM_FROM_DT"
  val PATIENT_ID_FIELD = "DESYNPUF_ID"
  val PRESCRIPTION_DATE_FIELD = "SRVC_DT"
  val NDC_ID_FIELD = "PROD_SRVC_ID"
  val DIABETES_INDEX_DATE = "DIABETES_INDEX_DATE"

  val inpatientFile = "data/raw/inpatient/inpatient.txt"
  val outpatientFile = "data/raw/outpatient/outpatient.txt"
  val carrierFile = "data/raw/carrier/carrier.txt"
  val beneficiaryFile = "data/raw/beneficiary/beneficiary.txt"
  val prescriptionFile = "data/raw/prescription/prescription.txt"
  val lovastatinFile = "data/lookup/lovastatin.txt"

  val lovastatin = Source.fromFile(lovastatinFile).getLines.toList

  // diagnosis column names
  val inpatientDiagnosisColNames = List("ADMTNG_ICD9_DGNS_CD", "ICD9_DGNS_CD_1", "ICD9_DGNS_CD_2", "ICD9_DGNS_CD_3", "ICD9_DGNS_CD_4", "ICD9_DGNS_CD_5", "ICD9_DGNS_CD_6", "ICD9_DGNS_CD_7", "ICD9_DGNS_CD_8", "ICD9_DGNS_CD_9", "ICD9_DGNS_CD_10")
  val outpatientDiagnosisColNames = List("ADMTNG_ICD9_DGNS_CD", "ICD9_DGNS_CD_1", "ICD9_DGNS_CD_2", "ICD9_DGNS_CD_3", "ICD9_DGNS_CD_4", "ICD9_DGNS_CD_5", "ICD9_DGNS_CD_6", "ICD9_DGNS_CD_7", "ICD9_DGNS_CD_8", "ICD9_DGNS_CD_9", "ICD9_DGNS_CD_10")
  val carrierDiagnosisColNames = List("ICD9_DGNS_CD_1", "ICD9_DGNS_CD_2", "ICD9_DGNS_CD_3", "ICD9_DGNS_CD_4", "ICD9_DGNS_CD_5", "ICD9_DGNS_CD_6", "ICD9_DGNS_CD_7", "ICD9_DGNS_CD_8", "LINE_ICD9_DGNS_CD_1", "LINE_ICD9_DGNS_CD_2", "LINE_ICD9_DGNS_CD_3", "LINE_ICD9_DGNS_CD_4", "LINE_ICD9_DGNS_CD_5", "LINE_ICD9_DGNS_CD_6", "LINE_ICD9_DGNS_CD_7", "LINE_ICD9_DGNS_CD_8", "LINE_ICD9_DGNS_CD_9", "LINE_ICD9_DGNS_CD_10", "LINE_ICD9_DGNS_CD_11", "LINE_ICD9_DGNS_CD_11", "LINE_ICD9_DGNS_CD_13")

  val yyyyddmmToAcceptableDateUDF = udf((s: String) =>
    if (s != null)
      s.substring(0, 4) + "-" + s.substring(4, 6) + "-" + s.substring(6, 8)
    else
      null)

  val age = udf((date: Date, dob: Date) => Period.between(dob.toLocalDate, date.toLocalDate).getYears)

  def diabeticPatients() = {
    val inpatient = spark.read.format("csv").option("header", "true").load(inpatientFile)
    val outpatient = spark.read.format("csv").option("header", "true").load(outpatientFile)
    val carrier = spark.read.format("csv").option("header", "true").load(carrierFile)
    val schema = StructType(StructField(PATIENT_ID_FIELD, DataTypes.StringType, true) :: Nil)
    var diabeticPatients = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    for (name <- inpatientDiagnosisColNames) {
      diabeticPatients = diabeticPatients.union(
        inpatient
          .filter(col(PATIENT_DATE_FIELD).startsWith("2009") && col(name).startsWith("250"))
          .select(col(PATIENT_ID_FIELD)))
    }
    for (name <- outpatientDiagnosisColNames) {
      diabeticPatients = diabeticPatients.union(
        outpatient
          .filter(col(PATIENT_DATE_FIELD).startsWith("2009") && col(name).startsWith("250"))
          .select(col(PATIENT_ID_FIELD)))
    }
    for (name <- carrierDiagnosisColNames) {
      diabeticPatients = diabeticPatients.union(
        carrier
          .filter(col(PATIENT_DATE_FIELD).startsWith("2009") && col(name).startsWith("250"))
          .select(col(PATIENT_ID_FIELD)))
    }
    diabeticPatients.dropDuplicates(PATIENT_ID_FIELD)
  }

  def dateIndexedDiabeticPatients(): DataFrame = {
    val inpatient = spark.read.format("csv").option("header", "true").load(inpatientFile)
    val outpatient = spark.read.format("csv").option("header", "true").load(outpatientFile)
    val carrier = spark.read.format("csv").option("header", "true").load(carrierFile)
    val schema = StructType(
      StructField(PATIENT_ID_FIELD, DataTypes.StringType, true) ::
        StructField(PATIENT_DATE_FIELD, DataTypes.StringType, true) ::
        StructField(DIABETES_INDEX_DATE, DataTypes.DateType, true) :: Nil)
    var datedDiabeticPatients = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    for (name <- inpatientDiagnosisColNames) {
      datedDiabeticPatients = datedDiabeticPatients.union(
        inpatient
          .filter(col(PATIENT_DATE_FIELD).startsWith("2009") && col(name).startsWith("250"))
          .select(col(PATIENT_ID_FIELD), col(PATIENT_DATE_FIELD), to_date(yyyyddmmToAcceptableDateUDF(col(PATIENT_DATE_FIELD)), "yyyy-MM-dd")))
    }
    for (name <- outpatientDiagnosisColNames) {
      datedDiabeticPatients = datedDiabeticPatients.union(
        outpatient
          .filter(col(PATIENT_DATE_FIELD).startsWith("2009") && col(name).startsWith("250"))
          .select(col(PATIENT_ID_FIELD), col(PATIENT_DATE_FIELD), to_date(yyyyddmmToAcceptableDateUDF(col(PATIENT_DATE_FIELD)), "yyyy-MM-dd")))
    }
    for (name <- carrierDiagnosisColNames) {
      datedDiabeticPatients = datedDiabeticPatients.union(
        carrier
          .filter(col(PATIENT_DATE_FIELD).startsWith("2009") && col(name).startsWith("250"))
          .select(col(PATIENT_ID_FIELD), col(PATIENT_DATE_FIELD), to_date(yyyyddmmToAcceptableDateUDF(col(PATIENT_DATE_FIELD)), "yyyy-MM-dd")))
    }
    datedDiabeticPatients = datedDiabeticPatients.groupBy(col(PATIENT_ID_FIELD))
      .agg(min(DIABETES_INDEX_DATE).as(DIABETES_INDEX_DATE))
      .drop(PATIENT_DATE_FIELD)
    datedDiabeticPatients
  }

  def lovastatinPrescriptions(): DataFrame = {
    val prescription = spark.read.format("csv").option("header", "true").load(prescriptionFile)
    prescription
      .select(col(PATIENT_ID_FIELD), to_date(yyyyddmmToAcceptableDateUDF(col(PRESCRIPTION_DATE_FIELD)), "yyyy-MM-dd").as(PRESCRIPTION_DATE_FIELD), col(NDC_ID_FIELD))
      .filter(col(NDC_ID_FIELD).isin(lovastatin: _*))
  }

  def dateIndexedDiabeticPatientsWithLovastatinPrescriptions(): DataFrame = {
    val patients = dateIndexedDiabeticPatients()
    val prescriptions = lovastatinPrescriptions()
    patients
      .withColumn("DT", add_months(col(DIABETES_INDEX_DATE), 12))
      .join(prescriptions, patients.col(PATIENT_ID_FIELD).equalTo(prescriptions.col(PATIENT_ID_FIELD)))
      .drop(prescriptions.col(PATIENT_ID_FIELD))
      .filter(col(PRESCRIPTION_DATE_FIELD).gt(col(DIABETES_INDEX_DATE)))
      .filter(col(PRESCRIPTION_DATE_FIELD).lt(col("DT")))
      .drop(col("DT"))
      .drop(col(PRESCRIPTION_DATE_FIELD))
      .drop(col(NDC_ID_FIELD))
  }

  def retiredDateIndexedDiabeticPatientsWithLovastatinPrescriptions(): DataFrame = {
    val patients = dateIndexedDiabeticPatientsWithLovastatinPrescriptions()
    val beneficiary = spark.read.format("csv").option("header", "true").load(beneficiaryFile)
    val retired = patients
      .join(beneficiary, patients.col(PATIENT_ID_FIELD).equalTo(beneficiary.col(PATIENT_ID_FIELD)))
      .drop(beneficiary.col("BENE_SEX_IDENT_CD"))
      .drop(beneficiary.col("BENE_RACE_CD"))
      .drop(beneficiary.col("BENE_ESRD_IND"))
      .drop(beneficiary.col("BENE_DEATH_DT"))
      .select(patients.col(PATIENT_ID_FIELD),
        patients.col(DIABETES_INDEX_DATE),
        to_date(yyyyddmmToAcceptableDateUDF(col("BENE_BIRTH_DT")), "yyyy-MM-dd").as("BENE_BIRTH_DT"),
      )
      .withColumn("AGE", age(col(DIABETES_INDEX_DATE), col("BENE_BIRTH_DT")))
    retired.filter(col("AGE").geq(65))
      .select(patients.col(PATIENT_ID_FIELD))
      .dropDuplicates(PATIENT_ID_FIELD)
  }

  def main(args: Array[String]): Unit = {
    // Node 1: Diabetes Patients -- 1740
    val node1 = diabeticPatients().count()

    // Node 2: Lovastatin -- 142
    val node2 = dateIndexedDiabeticPatientsWithLovastatinPrescriptions()
      .drop(col(DIABETES_INDEX_DATE))
      .dropDuplicates(PATIENT_ID_FIELD)
      .count()

    // Node 3: Patients Age > 65 -- 119
    val node3 = retiredDateIndexedDiabeticPatientsWithLovastatinPrescriptions().count()

    println(s"Section              Count")
    println(s"Node 1 (Diabetes):   $node1")
    println(s"Node 2 (Lovastatin): $node2")
    println(s"Node 3 (Age > 65):   $node3")
  }
}
