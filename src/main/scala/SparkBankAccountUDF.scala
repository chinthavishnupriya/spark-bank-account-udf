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
    // 2. BROADCAST ACCOUNT DATA
    // --------------------------------------------------

    val accountMap = accounts.collect()
      .map(account => account.accountId -> account)
      .toMap

    val broadcastAccounts =
      spark.sparkContext.broadcast(accountMap)

    println("\n===== BROADCAST VARIABLE =====")
    println(
      s"Broadcasted Accounts: ${broadcastAccounts.value.size}"
    )

    // --------------------------------------------------
    // 3. TRANSACTION DATA
    // One account can perform N transactions
    // --------------------------------------------------

    val transactions = Seq(

      // Chintan - 5 transactions
      Transaction(1001, "Chintan", "deposit", 2000.0, 1),
      Transaction(1001, "Chintan", "withdraw", 1000.0, 2),
      Transaction(1001, "Chintan", "deposit", 500.0, 3),
      Transaction(1001, "Chintan", "withdraw", 800.0, 4),
      Transaction(1001, "Chintan", "withdraw", 10000.0, 5),

      // Rahul - 3 transactions
      Transaction(1002, "Rahul", "deposit", 1000.0, 1),
      Transaction(1002, "Rahul", "withdraw", 2000.0, 2),
      Transaction(1002, "Rahul", "withdraw", 5000.0, 3),

      // Priya - 4 transactions
      Transaction(1003, "Priya", "deposit", 3000.0, 1),
      Transaction(1003, "Priya", "withdraw", 2000.0, 2),
      Transaction(1003, "Priya", "deposit", 1000.0, 3),
      Transaction(1003, "Priya", "withdraw", 5000.0, 4),

      // Amit - 3 transactions
      Transaction(1004, "Amit", "withdraw", 500.0, 1),
      Transaction(1004, "Amit", "deposit", 1000.0, 2),
      Transaction(1004, "Amit", "withdraw", 1000.0, 3)

    ).toDS()

    println("\n===== TRANSACTION DATA =====")
    transactions
      .orderBy("accountId", "transactionNo")
      .show(false)

    // --------------------------------------------------
    // 4. CONVERT TO DATAFRAMES
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
    // 5. JOIN ACCOUNT AND TRANSACTION DATA
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
    // 6. ACCUMULATORS
    // --------------------------------------------------

    val totalTransactions =
      spark.sparkContext.longAccumulator("Total Transactions")

    val successfulTransactions =
      spark.sparkContext.longAccumulator("Successful Transactions")

    val failedTransactions =
      spark.sparkContext.longAccumulator("Failed Transactions")

    val insufficientBalance =
      spark.sparkContext.longAccumulator("Insufficient Balance")

    val successfulDeposits =
      spark.sparkContext.longAccumulator("Successful Deposits")

    val successfulWithdrawals =
      spark.sparkContext.longAccumulator("Successful Withdrawals")

    // --------------------------------------------------
    // 7. USER DEFINED FUNCTION
    // --------------------------------------------------

    val processTransaction = udf(
      (
        currentBalance: Double,
        transactionType: String,
        amount: Double
      ) => {

        if (amount < 0) {

          (currentBalance, "Invalid Amount")

        } else if (
          transactionType.toLowerCase == "deposit"
        ) {

          (
            currentBalance + amount,
            "Deposit Successful"
          )

        } else if (
          transactionType.toLowerCase == "withdraw"
        ) {

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
    // 8. SEQUENTIAL TRANSACTION PROCESSING
    // USING BROADCAST ACCOUNT DATA
    // --------------------------------------------------

    val transactionRows = joinedDF
      .orderBy("accountId", "transactionNo")
      .collect()

    var currentAccountId = -1
    var currentBalance = 0.0

    val processedRows = transactionRows.map { row =>

      val accountId =
        row.getAs[Int]("accountId")

      val name =
        row.getAs[String]("name")

      val transactionType =
        row.getAs[String]("transactionType")

      val amount =
        row.getAs[Double]("amount")

      val transactionNo =
        row.getAs[Int]("transactionNo")

      // Get account information from Broadcast Variable
      val account =
        broadcastAccounts.value(accountId)

      // Start a new account with its initial balance
      if (accountId != currentAccountId) {

        currentAccountId = accountId

        currentBalance =
          account.initialBalance
      }

      // Count every transaction
      totalTransactions.add(1)

      val result =
        if (amount < 0) {

          failedTransactions.add(1)

          (
            currentBalance,
            "Invalid Amount"
          )

        } else if (
          transactionType.toLowerCase == "deposit"
        ) {

          successfulTransactions.add(1)
          successfulDeposits.add(1)

          (
            currentBalance + amount,
            "Deposit Successful"
          )

        } else if (
          transactionType.toLowerCase == "withdraw"
        ) {

          if (amount > currentBalance) {

            failedTransactions.add(1)
            insufficientBalance.add(1)

            (
              currentBalance,
              "Insufficient Balance"
            )

          } else {

            successfulTransactions.add(1)
            successfulWithdrawals.add(1)

            (
              currentBalance - amount,
              "Withdrawal Successful"
            )
          }

        } else {

          failedTransactions.add(1)

          (
            currentBalance,
            "Invalid Transaction"
          )
        }

      // Update running balance
      currentBalance = result._1

      (
        accountId,
        name,
        account.initialBalance,
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
    // 9. REGISTER UDF FOR SPARK SQL
    // --------------------------------------------------

    spark.udf.register(
      "transactionStatus",
      (
        transactionType: String,
        amount: Double,
        balanceBefore: Double
      ) => {

        if (amount < 0) {

          "Invalid Amount"

        } else if (
          transactionType.toLowerCase == "deposit"
        ) {

          "Deposit Successful"

        } else if (
          transactionType.toLowerCase == "withdraw"
        ) {

          if (amount > balanceBefore) {
            "Insufficient Balance"
          } else {
            "Withdrawal Successful"
          }

        } else {

          "Invalid Transaction"
        }
      }
    )

    // --------------------------------------------------
    // 10. CREATE TEMPORARY VIEW
    // --------------------------------------------------

    joinedDF.createOrReplaceTempView(
      "bank_transactions"
    )

    // --------------------------------------------------
    // 11. SPARK SQL + RUNNING BALANCE
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

          initialBalance
          +
          SUM(
            CASE
              WHEN transactionType = 'deposit'
                THEN amount
              ELSE -amount
            END
          ) OVER (
            PARTITION BY accountId
            ORDER BY transactionNo
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ) AS balanceBefore

        FROM bank_transactions
        ORDER BY accountId, transactionNo
      """
    )

    val sqlWithStatus = sqlResult
      .withColumn(
        "balanceBefore",
        coalesce(
          col("balanceBefore"),
          col("initialBalance")
        )
      )
      .withColumn(
        "status",
        expr(
          """
          transactionStatus(
            transactionType,
            amount,
            balanceBefore
          )
          """
        )
      )

    println("\n===== SQL RUNNING BALANCE + UDF =====")
    sqlWithStatus.show(false)

    // --------------------------------------------------
    // 12. FINAL RESULT
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
    // 13. ACCUMULATOR SUMMARY
    // --------------------------------------------------

    println("\n===== ACCUMULATOR SUMMARY =====")

    println(
      s"Total Transactions      : ${totalTransactions.value}"
    )

    println(
      s"Successful Transactions : ${successfulTransactions.value}"
    )

    println(
      s"Failed Transactions     : ${failedTransactions.value}"
    )

    println(
      s"Insufficient Balance    : ${insufficientBalance.value}"
    )

    println(
      s"Successful Deposits     : ${successfulDeposits.value}"
    )

    println(
      s"Successful Withdrawals  : ${successfulWithdrawals.value}"
    )

    // --------------------------------------------------
    // 14. SAVE FINAL OUTPUT
    // --------------------------------------------------

    finalResult
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(
        "output/final-bank-account-result"
      )

    println(
      "\n===== SPARK BANK ACCOUNT UDF COMPLETED ====="
    )

    // --------------------------------------------------
    // 15. CLEAN UP BROADCAST VARIABLE
    // --------------------------------------------------

    broadcastAccounts.destroy()

    spark.stop()
  }
}
