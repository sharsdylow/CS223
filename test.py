from math import floor

TRANSACTION_SIZE = 100
total_queries = 909

sqlCommands = [i for i in range(total_queries)]

transactions = [
    (
        sqlCommands[i : i + 100]
        if i < floor(total_queries / TRANSACTION_SIZE) * TRANSACTION_SIZE
        else sqlCommands[i : i + total_queries % TRANSACTION_SIZE]
    )
    for i in range(0, total_queries, TRANSACTION_SIZE)
]

print(transactions)
