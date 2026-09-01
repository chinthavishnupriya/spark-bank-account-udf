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
    // 1. ACCOUNT DATA
    // --------------------------------------------------

    val accounts = Seq(
      BankAccount(1001, "Chintan", 5000.0),
      BankAccount(1002, "Rahul", 3000.0),
      BankAccount(1003, "Priya", 10000.0),
      BankAccount(1004, "Amit", 2000.0)
    ).toDS()

    println("\n===== ACCOUNT DATA =====")
    accounts.show(false)

    // --------------------------------------------------
    // 2. TRANSACTION DATA
    // --------------------------------------------------

    val transactions = Seq(
      Transaction(1001, "Chintan", "deposit", 2000.0),
      Transaction(1002, "Rahul", "withdraw", 5000.0),
      Transaction(1003, "Priya", "deposit", 3000.0),
      Transaction(1004, "Amit", "withdraw", 500.0)
    ).toDS()

    println("\n===== TRANSACTION DATA =====")
    transactions.show(false)

    // --------------------------------------------------
    // 3. CONVERT TO DATAFRAMES
    // --------------------------------------------------

    val accountDF = accounts.toDF()
    val transactionDF = transactions.toDF()

    println("\n===== ACCOUNT DATAFRAME =====")
    accountDF.show(false)

    println("\n===== TRANSACTION DATAFRAME =====")
    transactionDF.show(false)

    // --------------------------------------------------
    // 4. JOIN ACCOUNT AND TRANSACTION DATA
    // --------------------------------------------------

    val joinedDF = accountDF
      .join(
        transactionDF,
        Seq("accountId", "name"),
        "inner"
      )

    println("\n===== JOINED DATA =====")
    joinedDF.show(false)

    // --------------------------------------------------
    // 5. USER DEFINED FUNCTION
    // --------------------------------------------------

    val processTransaction = udf(
      (initialBalance: Double,
       transactionType: String,
       amount: Double) => {

        if (amount < 0) {

          (initialBalance, "Invalid Amount")

        } else if (transactionType.toLowerCase == "deposit") {

          (
            initialBalance + amount,
            "Deposit Successful"
          )

        } else if (transactionType.toLowerCase == "withdraw") {

          if (amount > initialBalance) {

            (
              initialBalance,
              "Insufficient Balance"
            )

          } else {

            (
              initialBalance - amount,
              "Withdrawal Successful"
            )
          }

        } else {

          (
            initialBalance,
            "Invalid Transaction"
          )
        }
      }
    )

    // --------------------------------------------------
    // 6. APPLY UDF
    // --------------------------------------------------

    val resultDF = joinedDF
      .withColumn(
        "transactionResult",
        processTransaction(
          col("initialBalance"),
          col("transactionType"),
          col("amount")
        )
      )
      .withColumn(
        "finalBalance",
        col("transactionResult._1")
      )
      .withColumn(
        "status",
        col("transactionResult._2")
      )
      .drop("transactionResult")

    println("\n===== UDF RESULT =====")
    resultDF.show(false)

    // --------------------------------------------------
    // 7. REGISTER UDF FOR SPARK SQL
    // --------------------------------------------------

    spark.udf.register(
      "processBankTransaction",
      (
        initialBalance: Double,
        transactionType: String,
        amount: Double
      ) => {

        if (amount < 0) {

          (initialBalance, "Invalid Amount")

        } else if (transactionType.toLowerCase == "deposit") {

          (
            initialBalance + amount,
            "Deposit Successful"
          )

        } else if (transactionType.toLowerCase == "withdraw") {

          if (amount > initialBalance) {

            (
              initialBalance,
              "Insufficient Balance"
            )

          } else {

            (
              initialBalance - amount,
              "Withdrawal Successful"
            )
          }

        } else {

          (
            initialBalance,
            "Invalid Transaction"
          )
        }
      }
    )

    // --------------------------------------------------
    // 8. CREATE TEMPORARY VIEW
    // --------------------------------------------------

    joinedDF.createOrReplaceTempView("bank_transactions")

    // --------------------------------------------------
    // 9. SPARK SQL + UDF
    // --------------------------------------------------

    val sqlResult = spark.sql(
      """
        SELECT
          accountId,
          name,
          initialBalance,
          transactionType,
          amount,
          processBankTransaction(
            initialBalance,
            transactionType,
            amount
          ) AS transactionResult
        FROM bank_transactions
      """
    )

    println("\n===== SQL + UDF =====")
    sqlResult.show(false)

    // --------------------------------------------------
    // 10. FINAL RESULT
    // --------------------------------------------------

    val finalResult = resultDF.select(
      col("accountId"),
      col("name"),
      col("initialBalance"),
      col("transactionType"),
      col("amount"),
      col("finalBalance"),
      col("status")
    )

    println("\n===== FINAL RESULT =====")
    finalResult.show(false)

    // --------------------------------------------------
    // 11. SAVE OUTPUT
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
