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
    // Multiple transactions for the same account
    // --------------------------------------------------

    val transactions = Seq(
      Transaction(1001, "Chintan", "deposit", 2000.0, 1),
      Transaction(1001, "Chintan", "withdraw", 1000.0, 2),
      Transaction(1001, "Chintan", "deposit", 500.0, 3),
      Transaction(1001, "Chintan", "withdraw", 8000.0, 4),

      Transaction(1002, "Rahul", "withdraw", 5000.0, 1),
      Transaction(1002, "Rahul", "deposit", 1000.0, 2),

      Transaction(1003, "Priya", "deposit", 3000.0, 1),
      Transaction(1003, "Priya", "withdraw", 2000.0, 2),

      Transaction(1004, "Amit", "withdraw", 500.0, 1),
      Transaction(1004, "Amit", "deposit", 1000.0, 2)
    ).toDS()

    println("\n===== TRANSACTION DATA =====")
    transactions.orderBy("accountId", "transactionNo").show(false)

    // --------------------------------------------------
    // 3. CONVERT TO DATAFRAMES
    // --------------------------------------------------

    val accountDF = accounts.toDF()
    val transactionDF = transactions.toDF()

    println("\n===== ACCOUNT DATAFRAME =====")
    accountDF.show(false)

    println("\n===== TRANSACTION DATAFRAME =====")
    transactionDF
      .orderBy("accountId", "transactionNo")
      .show(false)

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
    joinedDF
      .orderBy("accountId", "transactionNo")
      .show(false)

    // --------------------------------------------------
    // 5. USER DEFINED FUNCTION
    // --------------------------------------------------

    val processTransaction = udf(
      (
        currentBalance: Double,
        transactionType: String,
        amount: Double
      ) => {

        if (amount < 0) {

          (currentBalance, "Invalid Amount")

        } else if (transactionType.toLowerCase == "deposit") {

          (
            currentBalance + amount,
            "Deposit Successful"
          )

        } else if (transactionType.toLowerCase == "withdraw") {

          if (amount > currentBalance) {

            (
              currentBalance,
              "Insufficient Balance"
            )

          } else {

            (
              currentBalance - amount,
              "Withdrawal Successful"
            )
          }

        } else {

          (
            currentBalance,
            "Invalid Transaction"
          )
        }
      }
    )

    // --------------------------------------------------
    // 6. PROCESS MULTIPLE TRANSACTIONS
    // --------------------------------------------------

    val transactionRows = joinedDF
      .orderBy("accountId", "transactionNo")
      .collect()

    var currentAccountId = -1
    var currentBalance = 0.0

    val processedRows = transactionRows.map { row =>

      val accountId = row.getAs[Int]("accountId")
      val name = row.getAs[String]("name")
      val initialBalance = row.getAs[Double]("initialBalance")
      val transactionType = row.getAs[String]("transactionType")
      val amount = row.getAs[Double]("amount")
      val transactionNo = row.getAs[Int]("transactionNo")

      if (accountId != currentAccountId) {
        currentAccountId = accountId
        currentBalance = initialBalance
      }

      val result =
        if (amount < 0) {

          (currentBalance, "Invalid Amount")

        } else if (transactionType.toLowerCase == "deposit") {

          (
            currentBalance + amount,
            "Deposit Successful"
          )

        } else if (transactionType.toLowerCase == "withdraw") {

          if (amount > currentBalance) {

            (
              currentBalance,
              "Insufficient Balance"
            )

          } else {

            (
              currentBalance - amount,
              "Withdrawal Successful"
            )
          }

        } else {

          (
            currentBalance,
            "Invalid Transaction"
          )
        }

      currentBalance = result._1

      (
        accountId,
        name,
        initialBalance,
        transactionNo,
        transactionType,
        amount,
        result._1,
        result._2
      )
    }

    val resultDF = processedRows.toSeq.toDF(
      "accountId",
      "name",
      "initialBalance",
      "transactionNo",
      "transactionType",
      "amount",
      "finalBalance",
      "status"
    )

    println("\n===== UDF RESULT =====")
    resultDF
      .orderBy("accountId", "transactionNo")
      .show(false)

    // --------------------------------------------------
    // 7. REGISTER UDF FOR SPARK SQL
    // --------------------------------------------------

    spark.udf.register(
      "processBankTransaction",
      (
        currentBalance: Double,
        transactionType: String,
        amount: Double
      ) => {

        if (amount < 0) {

          (currentBalance, "Invalid Amount")

        } else if (transactionType.toLowerCase == "deposit") {

          (
            currentBalance + amount,
            "Deposit Successful"
          )

        } else if (transactionType.toLowerCase == "withdraw") {

          if (amount > currentBalance) {

            (
              currentBalance,
              "Insufficient Balance"
            )

          } else {

            (
              currentBalance - amount,
              "Withdrawal Successful"
            )
          }

        } else {

          (
            currentBalance,
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
          transactionNo,
          transactionType,
          amount,
          processBankTransaction(
            initialBalance,
            transactionType,
            amount
          ) AS transactionResult
        FROM bank_transactions
        ORDER BY accountId, transactionNo
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
      col("transactionNo"),
      col("transactionType"),
      col("amount"),
      col("finalBalance"),
      col("status")
    )

    println("\n===== FINAL RESULT =====")

    finalResult
      .orderBy("accountId", "transactionNo")
      .show(false)

    // --------------------------------------------------
    // 11. SAVE FINAL OUTPUT
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
