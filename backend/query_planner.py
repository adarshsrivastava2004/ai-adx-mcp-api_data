# backend/query_planner.py
import requests
import re
import logging
from backend.config import OLLAMA_CHAT_URL, MODEL


# Setup Logger (Production Standard)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ENTERPRISE SYSTEM PROMPT (OPTIMIZED)
# Changes:
# 1. Enforced Case Insensitivity (=~)
# 2. Added "Performance First" rule (Time filters first)
# 3. Explicitly allowed 'let' statements
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """
You are a Principal Data Engineer and Azure Data Explorer (KQL) expert. Convert natural language requests into HIGH-PERFORMANCE, PRODUCTION-GRADE KQL queries.

## DATABASE SCHEMA: `API_gateway`

| Column Name                               | Type   | Description / Mapping Logic                                     |
|:------------------------------------------|:-------|:----------------------------------------------------------------|
| httpSessionID                             | string | Unique session tracker                                          |
| token                                     | string | **SENSITIVE**. Auth token. Use only if requested.               |
| source                                    | string | Source system identifier                                       |
| topicInBound                              | string | Inbound Kafka topic                                             |
| topicOutBound                             | string | Outbound Kafka topic                                            |
| topicReBound                              | string | Rebound Kafka topic                                             |
| sourcePOD                                 | string | The server handling the request                                 |
| messageReceivedTimeStamp                  | long   | **DEFAULT TIME COLUMN**. Unix Timestamp (ms).                   |
| messagePutIntoKafakTimeStamp              | long   | ACTUAL COLUMN NAME (typo in DB - DO NOT FIX)                    |
| messageReadFromKafakTimeStamp             | long   | ACTUAL COLUMN NAME (typo in DB - DO NOT FIX)                    |
| sourcePODIdentifictionDone                | string | Status of POD identification                                   |
| messageSendTimeStamp                      | long   | Unix Timestamp (ms). **MUST CONVERT TO DATETIME**.              |
| messageReadFromKafkaBySchTimeStamp        | long   | Unix Timestamp (ms). **MUST CONVERT TO DATETIME**.              |
| messagePuttedIntoKafkaBySchTimeStamp      | long   | ACTUAL COLUMN NAME (grammar error in DB - DO NOT FIX)           |
| messageOrigin                             | string | Origin of the message                                           |
| statusCode                                | string | HTTP status (200, 500). **STRING TYPE**.                        |
| statusDescription                         | string | Text description of status                                     |
| requestCounter                            | string | Request count identifier                                       |
| operation                                 | string | API Operation name/type                                        |
| corRelationId                             | string | NOTE CAMELCASE. Correlation ID.                                 |
| apiVersion                                | string | API Version (e.g., v1, v2)                                      |
| appVersion                                | string | Application Version                                            |
| recordId                                  | string | Unique Record ID                                               |
| msgBoardcast                              | string | Broadcast flag                                                 |
| actualmobilno                             | string | **PII** - Mobile number                                        |
| actualcustomerids                         | string | **PII** - Customer ID                                          |
| sessionref                                | string | Session reference                                              |
| messageReceivedByConnectionHoldingPOD     | string | Connection holding POD ID                                      |
| apiStatusCode                             | int    | Internal status code. **INTEGER TYPE**.                         |
| additionalinfo1                           | string | Custom metadata field 1                                        |
| additionalinfo2                           | string | Custom metadata field 2                                        |
| additionalinfo3                           | string | Custom metadata field 3                                        |
| additionalinfo4                           | string | Custom metadata field 4                                        |
| additionalinfo5                           | string | Custom metadata field 5                                        |
| responseBody                              | string | Full response text. Use ONLY if explicitly asked.               |
| errorMetaDat                              | string | ACTUAL COLUMN NAME (typo in DB - DO NOT FIX)                    |
| externalServiceLatency                    | string | **STRING TYPE - MUST CAST TO LONG**. Time in ms.                |
| x_forwarded_for                           | string | Primary IP Address column                                      |
| xforwardedFor                             | string | Secondary / Variant IP column                                  |
| deviceId                                  | string | User Device ID                                                 |
| userAgent                                 | string | Browser / Device info                                          |


## OUTPUT FORMAT (STRICT)
- Return **ONLY** raw KQL query
- **NO** markdown fences, explanations, or preambles
- Start directly with: `API_gateway`

## CRITICAL RULES

### String Matching (Case-Insensitive)
```
WRONG: where sourcePOD == "Pod-1"
RIGHT: where sourcePOD =~ "Pod-1"
RIGHT: where userAgent has "Chrome"
```

### Time Handling
ALL `*TimeStamp` columns are LONG (Unix ms). **MUST CONVERT**:
```
| where unixtime_milliseconds_todatetime(messageReceivedTimeStamp) > ago(24h)
| extend Time = unixtime_milliseconds_todatetime(messageReceivedTimeStamp)
```
- Apply time filter **FIRST** after table name
- If user doesn't specify time: **DO NOT ADD DEFAULT TIME FILTER**
- Valid units: ms, s, m, h, d (NOT: 1y, 1mo, 1w → use 365d, 30d, 7d)

### Type Casting
```
| extend Latency = tolong(externalServiceLatency)
| where isnotnull(Latency) and Latency > 0
| where statusCode != "200"    // String with quotes
| where apiStatusCode != 200   // Integer without quotes
```

### PII Protection
- Hash when grouping: `hash(actualcustomerids)`
- Include raw ONLY if explicitly requested

### Result Limits
- Raw queries: `| take 10000`
- Top N: `| top 1000 by ...`

### Syntax Blacklist
| BANNED (SQL) | USE (KQL) |
|--------------|-----------|
| countd() | dcount() |
| date_trunc() | bin() |
| Datediff(a,b) | a - b |
| NVL(), ISNULL() | iff(isnull(x), default, x) |

**NEVER** bin() on LONG directly: `bin(unixtime_milliseconds_todatetime(...), 1h)`

## QUERY PATTERNS

**Time-Series**: summarize by bin(Time, 1h) | render timechart
**Top N**: extend Latency = tolong(...) | top 50 by Latency desc
**Category**: summarize count() by sourcePOD | top 10 by ...
**Raw Data**: project columns | take 100
**Dashboard**: summarize TotalRequests, SuccessRate, AvgLatency, P95Latency

## OPTIMIZATION ORDER
1. TIME FILTER (first)
2. INDEXED FILTERS (sourcePOD, statusCode)
3. EXTEND (computed columns)
4. NON-INDEXED FILTERS
5. PROJECT
6. SUMMARIZE/TOP/ORDER

## VALIDATION CHECKLIST
✓ Time converted with unixtime_milliseconds_todatetime()
✓ Time filter FIRST after table
✓ String comparisons use =~ or has
✓ externalServiceLatency cast to long
✓ statusCode uses quotes
✓ PII protected unless requested
✓ Result limits applied
✓ Column names match schema (including typos)

If query is impossible, respond: `Cannot [operation] on [column]: [reason]`

"""

def generate_kql(user_goal: str, retry_count: int = 0, last_error: str = None) -> str:
    """
    Generates KQL, with AUTOMATIC SELF-HEALING if a previous attempt failed.
    Args:
        user_goal (str): The user's original natural language request.
        retry_count (int):
            - 0: Initial attempt (Standard Translation).
            - 1+: Retry attempt (Repair Mode).
        last_error (str): The specific error message from the database/compiler that caused the previous failure.
    """
    
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    
    # ---------------------------------------------------------
    # MODE 1: STANDARD GENERATION (First Attempt)
    # ---------------------------------------------------------
    # If this is the first time we are seeing this request, we just 
    # ask the LLM to translate the user's goal into KQL.
    if retry_count == 0:
        messages.append({"role": "user", "content": user_goal})
        logger.info(f"[QueryPlanner] Generating initial KQL for: {user_goal}")
        
    
    # ---------------------------------------------------------
    # MODE 2: REPAIR GENERATION (Self-Healing)
    # ---------------------------------------------------------
    # If we are retrying, it means the previous query failed.
    # Instead of asking the same question again (which would likely yield the same wrong answer),
    # we show the LLM the error message and ask it to debug its own code.
    else:
        logger.warning(f"[QueryPlanner] 🚑 Attempting Repair (Try #{retry_count}). Error: {last_error}")
        
        # We explicitly instruct the LLM to look at the error and fix the logic
        repair_prompt = f"""
        USER GOAL: {user_goal}
        
        PREVIOUS ATTEMPT FAILED.
        ERROR MESSAGE: {last_error}
        
        TASK: Fix the KQL query to resolve this specific error.
        - If the error is 'invalid data type', check your aggregations (bin/summarize).
        - If the error is 'syntax', check for missing pipes or brackets.
        - If the error is 'limit injected', rewrite the query to use 'summarize' instead of 'take'.
        - Output ONLY the fixed KQL.
        """
        messages.append({"role": "user", "content": repair_prompt})

    try:
        # json1={
        #         "model": MODEL,
        #         "messages": messages,
        #         "stream": False,
        #         "options": {"temperature": 0.1} # Strict precision
        #     }
        # print(json1)
        response = requests.post(
            OLLAMA_CHAT_URL,
            json={
                "model": MODEL,
                "messages": messages,
                "stream": False,
                "options": {"temperature": 0} # Strict precision
            },
            
            timeout=240
            
        )
        
        response.raise_for_status()
        
        data = response.json()
        raw_content = data.get("message", {}).get("content", "").strip()
        
        if not raw_content:
            logger.error("[QueryPlanner] LLM returned empty content.")
            return ""

        return sanitize_kql_output(raw_content)
         
    except Exception as e:
        logger.error(f"[QueryPlanner Error] {str(e)}", exc_info=True)
        return "" # Or re-raise depending on your API needs
    
def sanitize_kql_output(raw_text: str) -> str:
    """
    Extracts valid KQL from LLM noise.
    """
    # 1. Clean Markdown backticks immediately
    clean_text = raw_text.replace("```kql", "").replace("```", "").strip()

    # 2. Regex Strategy
    # Look for 'StormEventsCopy' OR 'let' (for variable declarations)
    # This prevents stripping valid variable definitions at the start.
    pattern = r"((?:let\s+.+?;\s*)?API_gateway.*)"
    match = re.search(pattern, clean_text, re.DOTALL | re.IGNORECASE)
    
    if match:
        kql = match.group(1).strip()
    else:
        # Fallback: Use the whole text if it looks vaguely like the table query
        # This handles cases where LLM might alias the table: "T | ..." (Rare but possible)
        kql = clean_text

    # 3. Final Integrity Check
    # We check if the table name exists strictly to avoid hallucinations
    if "API_gateway" not in kql:
        # Auto-Correction: If it looks like a pipe chain, prepend the table
        if kql.startswith("|"):
            kql = "API_gateway\n" + kql
        else:
            logger.error(f"[QueryPlanner] Invalid KQL generated: {kql}")
            return ""

    return kql
