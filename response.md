To fetch all users where **`ingestion_time()`** is older than 5 days
**and** the column `bba` is greater than 5, use the following **Kusto
Query Language (KQL)** query:

```kql
Users
| where ingestion_time() < ago(5d) and bba > 5
```

### Explanation:
1. **`ingestion_time() < ago(5d)`**
   - `ingestion_time()` returns the timestamp when the data was
ingested.
   - `ago(5d)` calculates the time 5 days before the current time.
   - This condition filters records ingested **more than 5 days ago**
(i.e., older than 5 days).

2. **`bba > 5`**
   - Filters rows where the numeric column `bba` is greater than 5.

### Notes:
- Replace `Users` with your actual table name if it differs.
- Ensure `bba` is a numeric column (e.g., integer or float).
- Time zones: `ago(5d)` uses the system's default time zone unless
specified otherwise. Adjust if needed.

Let me know if you need further refinements! 😊