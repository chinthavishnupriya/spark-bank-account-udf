# Spark Bank Account UDF

A Scala and Apache Spark project demonstrating **User Defined Functions (UDFs), Spark SQL, Window Functions, Accumulators, Broadcast Variables, and sequential transaction processing** for bank accounts.

The project supports **multiple transactions for the same account**. Each transaction is processed in sequence, and the balance from one transaction becomes the balance for the next transaction.

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

## 1. Account Data

The `BankAccount` case class contains:

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

## 2. Transaction Data

The `Transaction` case class contains:

* Account ID
* Name
* Transaction type
* Amount
* Transaction number

One account can perform **N transactions**.

For example, Chintan performs five transactions:

```text
Transaction 1 → Deposit  2000
Transaction 2 → Withdraw 1000
Transaction 3 → Deposit   500
Transaction 4 → Withdraw  800
Transaction 5 → Withdraw 10000
```

## 3. Sequential Transaction Processing

Transactions are processed according to their transaction number.

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

The current balance is carried forward to the next transaction.

The balance is never allowed to become negative.

## 4. User Defined Function

A Spark UDF named `processTransaction` is used to process transactions.

The UDF receives:

```text
currentBalance
transactionType
amount
```

It returns:

```text
new balance
transaction status
```

### Deposit

```text
new balance = current balance + amount
```

Status:

```text
Deposit Successful
```

### Withdrawal

If sufficient balance exists:

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

### Invalid Amount

Negative transaction amounts are rejected.

Status:

```text
Invalid Amount
```

### Invalid Transaction

Transactions other than `deposit` and `withdraw` are rejected.

Status:

```text
Invalid Transaction
```

## 5. Broadcast Variable

The project uses a **Spark Broadcast Variable** for account master data.

The account information is converted into a map:

```text
accountId → BankAccount
```

This map is broadcast to Spark workers.

The project verifies:

```text
Broadcasted Accounts: 4
```

### Why Broadcast?

Broadcast variables are useful when a relatively small piece of read-only data needs to be available to many worker tasks.

Instead of repeatedly transferring the same account data, Spark can distribute the data as a broadcast variable.

In this project, the broadcast data is used to retrieve the account's initial balance during transaction processing.

## 6. Accumulators

The project uses Spark **Long Accumulators** to collect transaction statistics.

The following accumulators are used:

```text
Total Transactions
Successful Transactions
Failed Transactions
Insufficient Balance
Successful Deposits
Successful Withdrawals
```

### Why Accumulators?

An accumulator allows worker tasks to add values to a shared counter that can be read by the driver program.

In this project, accumulators are used for **statistics and counting**, not for maintaining account balances.

The actual account balance remains business data and is processed separately.

## 7. Spark SQL

The joined account and transaction data is registered as a temporary view:

```text
bank_transactions
```

Spark SQL is then used to calculate the transaction's running balance information.

A registered SQL UDF named:

```text
transactionStatus
```

is used to determine the transaction status.

## 8. Window Function

A Spark SQL window function is used to calculate the balance before each transaction.

The window is partitioned by:

```text
accountId
```

and ordered by:

```text
transactionNo
```

This allows the project to demonstrate running-balance calculations for multiple transactions belonging to the same account.

## 9. Transaction Summary

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

## 10. Final Output

The final result contains:

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

Possible statuses include:

```text
Deposit Successful
Withdrawal Successful
Insufficient Balance
Invalid Amount
Invalid Transaction
```

The final result is saved as CSV.

## 11. Output Files

Console output:

```text
output/spark-bank-account-output.txt
```

Final CSV output:

```text
output/final-bank-account-result/
```

## 12. Running the Project

Compile:

```bash
sbt compile
```

Run:

```bash
sbt run
```

Save console output:

```bash
sbt run > output/spark-bank-account-output.txt 2>&1
```

Check the accumulator summary:

```bash
grep -A7 "ACCUMULATOR SUMMARY" output/spark-bank-account-output.txt
```

Check the broadcast variable:

```bash
grep "Broadcasted Accounts" output/spark-bank-account-output.txt
```

## 13. Git Workflow

Initialize the repository:

```bash
git init
```

Add files:

```bash
git add -A
```

Commit changes:

```bash
git commit -m "Commit message"
```

Push to GitHub:

```bash
git push
```

Check repository status:

```bash
git status
```

## 14. Learning Objectives

This project demonstrates:

1. Scala case classes
2. Spark Datasets
3. Spark DataFrames
4. DataFrame joins
5. User Defined Functions
6. Spark SQL
7. SQL UDF registration
8. Window functions
9. Multiple transactions per account
10. Sequential running balance
11. Deposit processing
12. Withdrawal processing
13. Insufficient-balance validation
14. Transaction validation
15. **Broadcast Variables**
16. **Accumulators**
17. Transaction statistics
18. CSV output
19. Git version control
20. GitHub project management

## 15. Project Status

**Completed**

The project demonstrates a complete Spark transaction-processing workflow using **Scala, Spark Dataset, DataFrame, UDF, Spark SQL, Window Functions, Broadcast Variables, Accumulators, sequential N-transaction processing, validation, transaction statistics, and CSV output.**
