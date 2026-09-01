# Spark Bank Account UDF

A Scala and Apache Spark project that demonstrates the use of
User Defined Functions (UDFs) for processing bank account
transactions.

## Project Objective

The project processes bank account information containing:

- Account ID
- Account holder name
- Initial balance
- Deposit amount
- Withdrawal amount

The application calculates the balance after deposit and
determines whether a withdrawal is successful or invalid.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.6
- Spark SQL
- sbt 2.0.7
- Java 17
- Ubuntu / WSL2

## Project Structure

```text
spark-bank-account-udf/
│
├── build.sbt
├── .gitignore
├── README.md
│
├── project/
│   └── build.properties
│
├── src/
│   └── main/
│       └── scala/
│           ├── BankAccount.scala
│           └── SparkBankAccountUDF.scala
│
└── output/
    ├── spark-bank-account-output.txt
    └── final-bank-account-result/
        └── part-00000-*.csv
