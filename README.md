# Spark Bank Account UDF

A Scala and Apache Spark project demonstrating a User Defined Function (UDF)
for processing bank account transactions.

## Project Objective

The project demonstrates how a custom UDF can process bank transactions
such as deposits and withdrawals.

The account information contains only:

- Account ID
- Account holder name
- Initial balance

Transaction information is maintained separately and contains:

- Account ID
- Account holder name
- Transaction type
- Transaction amount

The UDF processes the transaction and calculates the final balance.

## Technologies Used

- Java 17
- Scala 2.12.18
- Apache Spark 3.5.6
- Spark Core
- Spark SQL
- sbt 2.0.7
- Git
- GitHub

## Project Structure

```text
spark-bank-account-udf/
│
├── .gitignore
├── README.md
├── build.sbt
│
├── project/
│   └── build.properties
│
├── src/
│   └── main/
│       └── scala/
│           ├── BankAccount.scala
│           ├── Transaction.scala
│           └── SparkBankAccountUDF.scala
│
└── output/
    ├── spark-bank-account-output.txt
    └── final-bank-account-result/
        └── part-00000-*.csv
