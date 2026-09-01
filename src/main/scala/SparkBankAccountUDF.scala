import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkBankAccountUDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Bank Account UDF")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    println("===== SPARK BANK ACCOUNT UDF STARTED =====")

    // --------------------------------------------------
    // 1. CREATE DATASET
    // --------------------------------------------------

    val accounts = Seq(
      BankAccount(1001, "Chintan", 5000.0, 2000.0, 1000.0),
      BankAccount(1002, "Rahul", 3000.0, 1000.0, 5000.0),
      BankAccount(1003, "Priya", 10000.0, 5000.0, 3000.0),
      BankAccount(1004, "Amit", 2000.0, 500.0, 2500.0),
      BankAccount(1005, "Kiran", 4000.0, -500.0, 1000.0),
      BankAccount(1006, "Suresh", 6000.0, 1000.0, -200.0)
    ).toDS()

    println("\n===== DATASET =====")
    accounts.show()

    // --------------------------------------------------
    // 2. CONVERT DATASET TO DATAFRAME
    // --------------------------------------------------

    val accountDF = accounts.toDF()

    println("\n===== DATAFRAME =====")
    accountDF.show()

    // --------------------------------------------------
    // 3. CALCULATE BALANCE AFTER DEPOSIT
    // --------------------------------------------------

    val depositDF = accountDF.withColumn(
      "balanceAfterDeposit",
      when(col("deposit") < 0, col("initialBalance"))
        .otherwise(col("initialBalance") + col("deposit"))
    )

    println("\n===== BALANCE AFTER DEPOSIT =====")
    depositDF.show()

    // --------------------------------------------------
    // 4. USER-DEFINED FUNCTION
    // --------------------------------------------------

    val transactionStatus = udf(
      (balance: Double, withdraw: Double, deposit: Double) => {

        if (deposit < 0) {
          "Invalid Deposit"
        }
        else if (withdraw < 0) {
          "Invalid Withdrawal"
        }
        else if (withdraw > balance) {
          "Insufficient Balance"
        }
        else {
          "Withdrawal Successful"
        }
      }
    )

    // --------------------------------------------------
    // 5. APPLY UDF
    // --------------------------------------------------

    val resultDF = depositDF
      .withColumn(
        "status",
        transactionStatus(
          col("balanceAfterDeposit"),
          col("withdraw"),
          col("deposit")
        )
      )
      .withColumn(
        "finalBalance",
        when(
          col("status") === "Withdrawal Successful",
          col("balanceAfterDeposit") - col("withdraw")
        )
        .otherwise(col("balanceAfterDeposit"))
      )

    println("\n===== UDF RESULT =====")
    resultDF.show(false)

    // --------------------------------------------------
    // 6. REGISTER UDF FOR SPARK SQL
    // --------------------------------------------------

    spark.udf.register(
      "checkTransaction",
      (balance: Double, withdraw: Double, deposit: Double) => {

        if (deposit < 0) {
          "Invalid Deposit"
        }
        else if (withdraw < 0) {
          "Invalid Withdrawal"
        }
        else if (withdraw > balance) {
          "Insufficient Balance"
        }
        else {
          "Withdrawal Successful"
        }
      }
    )

    // --------------------------------------------------
    // 7. CREATE TEMPORARY VIEW
    // --------------------------------------------------

    resultDF.createOrReplaceTempView("bank_accounts")

    // --------------------------------------------------
    // 8. SPARK SQL + UDF
    // --------------------------------------------------

    val sqlResult = spark.sql(
      """
        SELECT
          accountId,
          name,
          initialBalance,
          deposit,
          withdraw,
          balanceAfterDeposit,
          finalBalance,
          checkTransaction(
            balanceAfterDeposit,
            withdraw,
            deposit
          ) AS transactionStatus
        FROM bank_accounts
      """
    )

    println("\n===== SQL + UDF =====")
    sqlResult.show(false)

    // --------------------------------------------------
    // 9. FINAL RESULT
    // --------------------------------------------------

    val finalResult = resultDF.select(
      col("accountId"),
      col("name"),
      col("initialBalance"),
      col("deposit"),
      col("withdraw"),
      col("finalBalance"),
      col("status")
    )

    println("\n===== FINAL RESULT =====")
    finalResult.show(false)

    // --------------------------------------------------
    // 10. SAVE OUTPUT
    // --------------------------------------------------

    finalResult
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/final-bank-account-result")

    println("\n===== SPARK BANK ACCOUNT UDF COMPLETED =====")

    spark.stop()
  }
}
