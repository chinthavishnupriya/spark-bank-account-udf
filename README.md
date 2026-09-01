# Spark Bank Account UDF

## Project Overview

This project demonstrates a simple **Bank Account Transaction Processing System** using **Apache Spark, Scala, DataFrames, Spark SQL, and User Defined Functions (UDFs)**.

The project stores basic account information separately from transaction information. Transactions are processed using a UDF to calculate the final balance and transaction status.

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.6
* Spark SQL
* Spark DataFrames
* Spark Datasets
* User Defined Functions (UDF)
* SBT
* Git and GitHub
* WSL2 / Linux

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
│           ├── Transaction.scala
│           └── SparkBankAccountUDF.scala
│
└── output/
    ├── spark-bank-account-output.txt
    └── final-bank-account-result/
        └── part-*.csv
```

## Account Data

The account table contains:

* Account ID
* Account Name
* Initial Balance

Example:

| Account ID | Name    | Initial Balance |
| ---------: | ------- | --------------: |
|       1001 | Chintan |          5000.0 |
|       1002 | Rahul   |          3000.0 |
|       1003 | Priya   |         10000.0 |
|       1004 | Amit    |          2000.0 |

## Transaction Data

Transactions are stored separately and contain:

* Account ID
* Name
* Transaction Type
* Amount
* Transaction Number

Example transactions include:

* Deposit
* Withdrawal
* Withdrawal greater than available balance

## Processing Flow

```text
Account Data
     ↓
Transaction Data
     ↓
Convert to Dataset
     ↓
Convert to DataFrame
     ↓
Join Account + Transaction
     ↓
Apply User Defined Function
     ↓
Validate Transaction
     ↓
Calculate Final Balance
     ↓
Spark SQL + UDF
     ↓
Final Result
     ↓
Transaction Summary
     ↓
Save CSV Output
```

## UDF Logic

The UDF processes each transaction according to its type.

### Deposit

```text
Final Balance = Current Balance + Deposit Amount
```

Status:

```text
Deposit Successful
```

### Withdrawal

If the withdrawal amount is less than or equal to the available balance:

```text
Final Balance = Current Balance - Withdrawal Amount
```

Status:

```text
Withdrawal Successful
```

If the withdrawal amount is greater than the available balance:

```text
Final Balance = Current Balance
```

Status:

```text
Insufficient Balance
```

The program prevents the account balance from becoming negative.

### Invalid Amount

Negative transaction amounts are rejected.

Status:

```text
Invalid Amount
```

### Invalid Transaction

Unknown transaction types are rejected.

Status:

```text
Invalid Transaction
```

## Current Test Transactions

| Account | Transaction | Amount | Result                |
| ------- | ----------- | -----: | --------------------- |
| Chintan | Deposit     | 2000.0 | Deposit Successful    |
| Rahul   | Withdraw    | 5000.0 | Insufficient Balance  |
| Priya   | Deposit     | 3000.0 | Deposit Successful    |
| Amit    | Withdraw    |  500.0 | Withdrawal Successful |

## Final Results

| Account | Initial Balance | Type     | Amount | Final Balance | Status                |
| ------- | --------------: | -------- | -----: | ------------: | --------------------- |
| Chintan |          5000.0 | Deposit  | 2000.0 |        7000.0 | Deposit Successful    |
| Rahul   |          3000.0 | Withdraw | 5000.0 |        3000.0 | Insufficient Balance  |
| Priya   |         10000.0 | Deposit  | 3000.0 |       13000.0 | Deposit Successful    |
| Amit    |          2000.0 | Withdraw |  500.0 |        1500.0 | Withdrawal Successful |

## Transaction Summary

The project also calculates transaction statistics using Spark DataFrame operations.

```text
Total Transactions      : 4
Successful Transactions : 3
Failed Transactions     : 1
Insufficient Balance    : 1
Successful Deposits     : 2
Successful Withdrawals  : 1
```

## Spark SQL

The UDF is also registered with Spark SQL:

```text
processBankTransaction
```

A temporary view is created:

```text
bank_transactions
```

The registered UDF is then called from a Spark SQL query to process transaction data.

## Output

The program saves the final transaction result as a CSV file:

```text
output/final-bank-account-result/
```

The complete console output is also stored in:

```text
output/spark-bank-account-output.txt
```

## How to Run

### Compile

```bash
sbt compile
```

### Run

```bash
sbt run
```

### Save Console Output

```bash
sbt run > output/spark-bank-account-output.txt 2>&1
```

## GitHub

Repository:

`https://github.com/chinthavishnupriya/spark-bank-account-udf`

## Learning Outcomes

This project demonstrates:

* Creating Scala case classes
* Creating Spark Datasets
* Converting Datasets to DataFrames
* Joining DataFrames
* Creating and using UDFs
* Registering UDFs with Spark SQL
* Creating temporary SQL views
* Performing transaction validation
* Calculating final account balances
* Preventing invalid withdrawals
* Performing transaction statistics
* Saving Spark results as CSV
* Managing a Spark project with SBT
* Version control using Git and GitHub
