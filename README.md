# Spark Bank Account UDF

A Scala and Apache Spark project that demonstrates how **User Defined Functions (UDFs)** can be used to process bank account transactions.

The project supports **multiple transactions for the same account**. Each transaction is processed sequentially, and the balance from one transaction becomes the balance for the next transaction.

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.6
* Spark SQL
* sbt
* Java 17
* Git and GitHub

## Project Structure

```text
spark-bank-account-udf/
│
├── build.sbt
├── README.md
├── .gitignore
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
        └── part-*.csv
```

## Account Data

The account data contains:

* Account ID
* Account holder name
* Initial balance

Example:

```text
Account ID    Name       Initial Balance
1001          Chintan    5000.0
1002          Rahul      3000.0
1003          Priya      10000.0
1004          Amit       2000.0
```

## Transaction Data

A separate transaction dataset is used.

Each transaction contains:

* Account ID
* Name
* Transaction type
* Amount
* Transaction number

One account can perform **N transactions**.

For example, Chintan has five transactions:

```text
Transaction 1 → Deposit  2000
Transaction 2 → Withdraw 1000
Transaction 3 → Deposit   500
Transaction 4 → Withdraw  800
Transaction 5 → Withdraw 10000
```

## Sequential Balance Processing

Transactions are processed in transaction-number order.

For Chintan:

```text
Initial Balance = 5000

Transaction 1:
5000 + 2000 = 7000

Transaction 2:
7000 - 1000 = 6000

Transaction 3:
6000 + 500 = 6500

Transaction 4:
6500 - 800 = 5700

Transaction 5:
Withdraw 10000

10000 > 5700
Result = Insufficient Balance

Final Balance = 5700
```

The balance is therefore carried forward from one transaction to the next.

## UDF Logic

The project uses a User Defined Function to process transactions.

### Deposit

If the transaction is a deposit:

```text
new balance = current balance + amount
```

Status:

```text
Deposit Successful
```

### Withdrawal

If the transaction is a withdrawal and sufficient balance exists:

```text
new balance = current balance - amount
```

Status:

```text
Withdrawal Successful
```

### Insufficient Balance

If the withdrawal amount is greater than the current balance:

```text
final balance = current balance
```

Status:

```text
Insufficient Balance
```

The balance is never allowed to become negative.

### Invalid Amount

Negative transaction amounts are rejected.

Status:

```text
Invalid Amount
```

### Invalid Transaction

Transactions other than `deposit` or `withdraw` are rejected.

Status:

```text
Invalid Transaction
```

## Spark SQL

The project also demonstrates Spark SQL integration.

A temporary view is created from the joined account and transaction data.

Spark SQL and a registered UDF are used to demonstrate transaction-status processing and running balance information.

## Transaction Summary

The current test dataset contains **15 transactions**.

```text
Total Transactions      : 15
Successful Transactions : 13
Failed Transactions     : 2
Insufficient Balance    : 2
Successful Deposits     : 6
Successful Withdrawals  : 7
```

### Transactions Per Account

```text
Chintan → 5 transactions
Rahul   → 3 transactions
Priya   → 4 transactions
Amit    → 3 transactions

Total    → 15 transactions
```

## Final Result

The final output contains:

```text
accountId
name
initialBalance
transactionNo
transactionType
amount
finalBalance
status
```

Example transaction statuses include:

```text
Deposit Successful
Withdrawal Successful
Insufficient Balance
```

## Running the Project

Compile the project:

```bash
sbt compile
```

Run the project:

```bash
sbt run
```

To save the console output:

```bash
sbt run > output/spark-bank-account-output.txt 2>&1
```

Check the transaction summary:

```bash
grep -A7 "TRANSACTION SUMMARY" output/spark-bank-account-output.txt
```

## Output

The project saves the final result as a CSV file in:

```text
output/final-bank-account-result/
```

The complete console output is saved in:

```text
output/spark-bank-account-output.txt
```

## Learning Objectives

This project demonstrates:

1. Creating Scala case classes.
2. Creating Spark Datasets.
3. Converting Datasets into DataFrames.
4. Joining account and transaction data.
5. Creating and using Spark UDFs.
6. Registering UDFs for Spark SQL.
7. Processing multiple transactions for one account.
8. Maintaining a sequential running balance.
9. Validating deposits and withdrawals.
10. Handling insufficient balance.
11. Generating transaction summaries.
12. Saving Spark results as CSV.
13. Managing the project using Git and GitHub.

## Project Status

**Completed**

The project demonstrates a complete Spark UDF workflow for bank accounts with **multiple sequential transactions per account**, transaction validation, insufficient-balance handling, Spark SQL integration, summary statistics, and CSV output.
