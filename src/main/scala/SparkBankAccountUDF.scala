import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkBankAccountUDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Bank Account UDF")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("===== SPARK BANK ACCOUNT UDF STARTED =====")

    import spark.implicits._

    // --------------------------------------------------
    // 1. Create bank account data
    // --------------------------------------------------

    val accounts = Seq(
      BankAccount(1001, "Chintan", 5000.0, 2000.0, 1000.0),
      BankAccount(1002, "Rahul", 3000.0, 1000.0, 5000.0),
      BankAccount(1003, "Priya", 10000.0, 5000.0, 3000.0),
      BankAccount(1004, "Amit", 2000.0, 500.0, 2500.0)
    )

    val dataset = spark.createDataset(accounts)

    println("\n===== DATASET =====")
    dataset.show()

    // --------------------------------------------------
    // 2. Convert Dataset to DataFrame
    // --------------------------------------------------

    val df = dataset.toDF()

    println("\n===== DATAFRAME =====")
    df.show()

    // --------------------------------------------------
    // 3. Calculate balance after deposit
    // --------------------------------------------------

    val balanceAfterDeposit = df.withColumn(
      "balanceAfterDeposit",
      col("initialBalance") + col("deposit")
    )

    println("\n===== BALANCE AFTER DEPOSIT =====")
    balanceAfterDeposit.show()

    // --------------------------------------------------
    // 4. Create User Defined Function
    // --------------------------------------------------

    val withdrawStatus = udf((balance: Double, withdraw: Double) => {

      if (withdraw <= balance) {
        "Withdrawal Successful"
      } else {
        "Insufficient Balance"
      }

    })

    // --------------------------------------------------
    // 5. Register UDF with Spark
    // --------------------------------------------------

    spark.udf.register(
      "checkwithdrawal",
      (balance: Double, withdraw: Double) => {

        if (withdraw <= balance) {
          "Withdrawal Successful"
        } else {
          "Insufficient Balance"
        }

      }
    )

    // --------------------------------------------------
    // 6. Apply UDF to DataFrame
    // --------------------------------------------------

    val result = balanceAfterDeposit
      .withColumn(
        "status",
        withdrawStatus(
          col("balanceAfterDeposit"),
          col("withdraw")
        )
      )
      .withColumn(
        "finalBalance",
        when(
          col("withdraw") <= col("balanceAfterDeposit"),
          col("balanceAfterDeposit") - col("withdraw")
        ).otherwise(col("balanceAfterDeposit"))
      )

    println("\n===== UDF RESULT =====")
    result.show()

    // --------------------------------------------------
    // 7. Create temporary SQL view
    // --------------------------------------------------

    result.createOrReplaceTempView("bank_accounts")

    // --------------------------------------------------
    // 8. Use UDF through Spark SQL
    // --------------------------------------------------

    println("\n===== SQL + UDF =====")

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
          checkwithdrawal(balanceAfterDeposit, withdraw) AS transactionStatus
        FROM bank_accounts
      """
    )

    sqlResult.show(false)

    // --------------------------------------------------
    // 9. Display final result
    // --------------------------------------------------

    println("\n===== FINAL RESULT =====")

    result
      .select(
        col("accountId"),
        col("name"),
        col("initialBalance"),
        col("deposit"),
        col("withdraw"),
        col("finalBalance"),
        col("status")
      )
      .show(false)

    println("\n===== SPARK BANK ACCOUNT UDF COMPLETED =====")

    spark.stop()
  }
}
