# backend/query_planner.py
import requests
import re
import logging
from backend.config import OLLAMA_CHAT_URL, MODEL
import time

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
You are an expert Azure Data Explorer (KQL) assistant.
Your goal: Convert the user's natural language question into a syntacticly correct, read-only KQL query for the table `API_gateway`.


### CRITICAL RULES (Follow these or the query will fail)
1.  **NO SQL:** Do not use SELECT, FROM, WHERE (SQL style), or DATE functions like year(), month(), date().
2.  **TIME HANDLING:**
    - The database stores time as Unix Milliseconds (Long).
    - YOU MUST convert time columns using: `unixtime_milliseconds_todatetime(ColumnName)`.
    - For "Last X hours/days": Use `> ago(X)`.
    - For "Today": Use `>= startofday(now())` AND `< startofday(now()) + 1d`.
3.  **EXACT COLUMN NAMES:** The schema contains typos. YOU MUST use the exact column names below, do not correct them.
    - `messagePutIntoKafakTimeStamp` (Keep 'Kafak')
    - `messagePuttedIntoKafkaBySchTimeStamp` (Keep 'Putted')
    - `actualmobilno` (Keep lowercase)
4.  **STRING LITERALS:**
    - `statusCode` is a string. Always quote it: `statusCode == "200"`.
    - `apiStatusCode` is an int. Never quote it: `apiStatusCode == 200`.

### SCHEMA (Table: API_gateway)
| Column Name                           | Type   | Critical Notes                                            |
|:--------------------------------------|:-------|:----------------------------------------------------------|
| httpSessionID                         | string | Unique session tracker                                    |
| token                                 | string | SENSITIVE — use ONLY if explicitly requested              |
| source                                | string | Source system identifier                                  |
| topicInBound                          | string | Inbound Kafka topic                                       |
| topicOutBound                         | string | Outbound Kafka topic                                      |
| topicReBound                          | string | Rebound Kafka topic                                       |
| sourcePOD                             | string | Server handling the request                               |
| messageReceivedTimeStamp              | long   | ⭐ DEFAULT TIME COLUMN. Unix ms. MUST CONVERT TO DATETIME  |
| messagePutIntoKafakTimeStamp          | long   | TYPO IN DB — do not rename. Unix ms. MUST CONVERT         |
| messageReadFromKafakTimeStamp         | long   | TYPO IN DB — do not rename. Unix ms. MUST CONVERT         |
| sourcePODIdentifictionDone            | string | Status of POD identification                              |
| messageSendTimeStamp                  | long   | Unix ms. MUST CONVERT TO DATETIME                         |
| messageReadFromKafkaBySchTimeStamp    | long   | Unix ms. MUST CONVERT TO DATETIME                         |
| messagePuttedIntoKafkaBySchTimeStamp  | long   | GRAMMAR ERROR IN DB — do not rename. Unix ms. MUST CONVERT|
| messageOrigin                         | string | Origin of the message                                     |
| statusCode                            | string | ⚠️ STRING TYPE — always use quotes: "200", "500"           |
| statusDescription                     | string | Text description of status                                |
| requestCounter                        | string | Request count identifier                                  |
| operation                             | string | API operation name                                        |
| corRelationId                         | string | ⚠️ CAMELCASE — correlation ID                              |
| apiVersion                            | string | API version (v1, v2)                                      |
| appVersion                            | string | App version                                               |
| recordId                              | string | Unique record ID                                          |
| msgBoardcast                          | string | Broadcast flag                                            |
| actualmobilno                         | string | 🔒 PII — mobile number. Hash unless raw explicitly asked   |
| actualcustomerids                     | string | 🔒 PII — customer ID. Hash unless raw explicitly asked     |
| sessionref                            | string | Session reference                                         |
| messageReceivedByConnectionHoldingPOD | string | Connection holding POD ID                                 |
| apiStatusCode                         | int    | ⚠️ INTEGER TYPE — never use quotes: 200 not "200"          |
| additionalinfo1                       | string | Custom metadata 1                                         |
| additionalinfo2                       | string | Custom metadata 2                                         |
| additionalinfo3                       | string | Custom metadata 3                                         |
| additionalinfo4                       | string | Custom metadata 4                                         |
| additionalinfo5                       | string | Custom metadata 5                                         |
| responseBody                          | string | Full response — use ONLY if explicitly asked              |
| errorMetaDat                          | string | TYPO IN DB — do not rename                                |
| externalServiceLatency                | string | ⚠️ STRING TYPE — MUST cast to long before math             |
| x_forwarded_for                       | string | Primary IP column                                         |
| xforwardedFor                         | string | Secondary/variant IP column                               |
| deviceId                              | string | Device ID                                                 |
| userAgent                             | string | Browser/device info                                       |

### REFERENCE EXAMPLES (Pattern Match These)

**User:** "Count requests by operation and status"
**KQL:**
API_gateway
| where unixtime_milliseconds_todatetime(messageReceivedTimeStamp) > ago(24h)
| summarize count() by operation, statusCode


**User:** "Count unique mobile numbers"
**KQL:**
API_gateway
| where unixtime_milliseconds_todatetime(messageReceivedTimeStamp) > ago(24h)
| summarize count() by hash(actualmobilno)

**User:** "Find ingestion delays for customer demo details"
**KQL:**
API_gateway
| where ingestion_time() > ago(1d)
| where operation has "custdemogdetails"
| take 100

User: "Show me all failed requests from today"
KQL:
API_gateway
| where unixtime_milliseconds_todatetime(messageReceivedTimeStamp) >= startofday(now())
| where unixtime_milliseconds_todatetime(messageReceivedTimeStamp) < startofday(now()) + 1d
| where statusCode != "200"

User: "Calculate average external latency by source"
KQL:
API_gateway
| where unixtime_milliseconds_todatetime(messageReceivedTimeStamp) > ago(24h)
| extend latency = tolong(externalServiceLatency)
| summarize avg(latency) by source

User: "Check Kafka put timestamps for the last hour"
KQL:
API_gateway
| where unixtime_milliseconds_todatetime(messageReceivedTimeStamp) > ago(1h)
| project unixtime_milliseconds_todatetime(messagePutIntoKafakTimeStamp), unixtime_milliseconds_todatetime(messagePuttedIntoKafkaBySchTimeStamp)

User: "Count API errors by integer status code"
KQL:
API_gateway
| where unixtime_milliseconds_todatetime(messageReceivedTimeStamp) > ago(24h)
| where apiStatusCode != 200
| summarize count() by apiStatusCode

### Rules:
- Time filter MUST be the FIRST pipe operator after the table name
- If no time is specified: DO NOT add any time filter
- Valid ago() units: ms, s, m, h, d — NOT: 1y, 1mo, 1w → use 365d, 30d, 7d
- NEVER use date(), todays_date(), current_date(), today() — they do not exist in KQL
- NEVER use ago() when user says "today" — use startofday(now()) instead
- NEVER use == for case-insensitive matching. ALWAYS use =~ when case is unknown.

Always write pipes in this order:
1. Table name: API_gateway
2. TIME FILTER (first pipe, always)
3. INDEXED FILTERS: sourcePOD, statusCode, operation, apiStatusCode
4. EXTEND: computed columns (EventTime, Latency, etc.)
5. NON-INDEXED FILTERS: filters on extended columns
6. PROJECT: column selection (raw queries only)
7. SUMMARIZE / TOP / ORDER BY
8. RENDER (if chart requested)

## OUTPUT FORMAT (STRICT)

- Return ONLY the raw KQL query
- NO markdown code fences (no ```kql or ```)
- NO explanations, preambles, or notes
- NO comments inside the query
- Start the response directly with: API_gateway
- If the request is impossible to fulfill, respond only with: Cannot [operation] on [column]: [reason]

"""

def generate_kql(user_goal: str, retry_count: int = 0, last_error: str = None, last_kql: str = None) -> str:
    """
    Generates KQL, with AUTOMATIC SELF-HEALING if a previous attempt failed.
    Args:
        user_goal (str): The user's original natural language request.
        retry_count (int):
            - 0: Initial attempt (Standard Translation).
            - 1+: Retry attempt (Repair Mode).
        last_error (str): The specific error message from the database/compiler that caused the previous failure.
    """
    
    # ------------------------------------------------------------------
    # STEP 1: Function Entry
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info(f"[QueryPlanner] STEP 1 — generate_kql() called")
    logger.info(f"[QueryPlanner]   ├─ user_goal   : {user_goal!r}")
    logger.info(f"[QueryPlanner]   ├─ retry_count : {retry_count}")
    logger.info(f"[QueryPlanner]   └─ last_error  : {last_error!r}")

    # ------------------------------------------------------------------
    # STEP 2: Build Message List
    # ------------------------------------------------------------------
    logger.info(f"[QueryPlanner] STEP 2 — Building message payload (system prompt always included)")
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    logger.debug(f"[QueryPlanner]   └─ System prompt length: {len(SYSTEM_PROMPT)} chars")
    # ---------------------------------------------------------
    # MODE 1: STANDARD GENERATION (First Attempt)
    # ---------------------------------------------------------
    # If this is the first time we are seeing this request, we just 
    # ask the LLM to translate the user's goal into KQL.
    if retry_count == 0:
        logger.info(f"[QueryPlanner] STEP 3 — Mode: STANDARD GENERATION (first attempt)")
        messages.append({"role": "user", "content": f"generate KQL : {user_goal}"})
        logger.info(f"[QueryPlanner] Generating initial KQL for: {user_goal}")
        
    
    # ---------------------------------------------------------
    # MODE 2: REPAIR GENERATION (Self-Healing)
    # ---------------------------------------------------------
    # If we are retrying, it means the previous query failed.
    # Instead of asking the same question again (which would likely yield the same wrong answer),
    # we show the LLM the error message and ask it to debug its own code.
    else:
        logger.warning(f"[QueryPlanner] STEP 3 — Mode: REPAIR / SELF-HEALING (attempt #{retry_count})")
        logger.warning(f"[QueryPlanner]   └─ Triggering error was: {last_error!r}")

        # We explicitly instruct the LLM to look at the error and fix the logic
        repair_prompt = f"""
        USER GOAL: {user_goal}

        FAILED KQL:
        {last_kql or "unavailable"}   # <-- ADD THIS: pass the broken query in

        ERROR MESSAGE: {last_error}

        KNOWN ILLEGAL KQL FUNCTIONS (DO NOT USE):
        - date()       → use startofday()
        - todays_date() → use startofday(now())
        - current_date() → use startofday(now())
        - count_different() → use dcount()

        Output ONLY the fixed KQL.
        """
        messages.append({"role": "user", "content": repair_prompt})
        logger.debug(f"[QueryPlanner]   └─ Repair prompt constructed ({len(repair_prompt)} chars)")

        
    # ------------------------------------------------------------------
    # STEP 4: Call LLM
    # ------------------------------------------------------------------
    logger.info(f"[QueryPlanner] STEP 4 — Sending request to LLM")
    logger.info(f"[QueryPlanner]   ├─ URL   : {OLLAMA_CHAT_URL}")
    logger.info(f"[QueryPlanner]   ├─ Model : {MODEL}")
    logger.info(f"[QueryPlanner]   └─ Total messages in payload: {len(messages)}")

    try:
        start_time = time.time()
        response = requests.post(
            OLLAMA_CHAT_URL,
            json={
                "model": MODEL,
                "messages": messages,
                "stream": False,
                "options": {"temperature": 0,
                            "num_ctx": 8192} # Strict precision
            },
            timeout=240
        )
        # print(messages)
        elapsed = time.time() - start_time
        logger.info(f"[QueryPlanner] STEP 5 — LLM responded in {elapsed:.2f}s | HTTP status: {response.status_code}")
        
        # ------------------------------------------------------------------
        # STEP 5: Validate HTTP Response
        # ------------------------------------------------------------------
        response.raise_for_status()
        logger.info(f"[QueryPlanner]   └─ HTTP response OK (raise_for_status passed)")
        
        
        # ------------------------------------------------------------------
        # STEP 6: Parse JSON Body
        # ------------------------------------------------------------------
        logger.info(f"[QueryPlanner] STEP 6 — Parsing JSON response body")
        data = response.json()
        raw_content = data.get("message", {}).get("content", "").strip()
        
        if not raw_content:
            logger.error("[QueryPlanner] LLM returned empty content.")
            return ""
        logger.info(f"[QueryPlanner]   KQl content received ({raw_content})")
        logger.info(f"[QueryPlanner]   └─ Raw LLM content received ({len(raw_content)} chars)")
        logger.debug(f"[QueryPlanner]   └─ Raw content preview: {raw_content[:300]!r}{'...' if len(raw_content) > 300 else ''}")


        # ------------------------------------------------------------------
        # STEP 7: Sanitize Output
        # ------------------------------------------------------------------
        logger.info(f"[QueryPlanner] STEP 7 — Sanitizing KQL output")
        kql = sanitize_kql_output(raw_content)
        
        
        if kql:
            logger.info(f"[QueryPlanner]   └─ Sanitization SUCCESS — final KQL ({len(kql)} chars)")
            logger.debug(f"[QueryPlanner]   └─ Final KQL:\n{kql}")
        else:
            logger.error(f"[QueryPlanner]   └─ Sanitization FAILED — empty result returned")
            
            
            
        logger.info(f"[QueryPlanner] STEP 8 — generate_kql() complete")
        logger.info("=" * 60)
        return kql
    
    except requests.exceptions.Timeout:
        logger.error(f"[QueryPlanner] STEP 5 — LLM request TIMED OUT after 240s")
        logger.info("=" * 60)
        return ""
    
    except requests.exceptions.HTTPError as e:
        logger.error(f"[QueryPlanner] STEP 5 — HTTP error from LLM: {e} | Response: {e.response.text[:300]}")
        logger.info("=" * 60)
        return ""
    
    except Exception as e:
        logger.error(f"[QueryPlanner] STEP 5 — Unexpected error: {str(e)}", exc_info=True)
        logger.info("=" * 60)
        return ""

def sanitize_kql_output(raw_text: str) -> str:
    """
    Extracts valid KQL from LLM noise.
    """
    logger.debug(f"[Sanitizer] STEP 7a — Stripping markdown fences")
    
    
    # ------------------------------------------------------------------
    # SUB-STEP 7a: Strip markdown code fences
    # ------------------------------------------------------------------
    clean_text = raw_text.replace("```kql", "").replace("```", "").strip()
    logger.debug(f"[Sanitizer]   └─ After fence strip ({len(clean_text)} chars): {clean_text[:200]!r}")


    # ------------------------------------------------------------------
    # SUB-STEP 7b: Regex extraction
    # ------------------------------------------------------------------
    logger.debug(f"[Sanitizer] STEP 7b — Running regex to extract KQL block")
    pattern = r"((?:let\s+.+?;\s*)?API_gateway.*)"
    match = re.search(pattern, clean_text, re.DOTALL | re.IGNORECASE)

    if match:
        kql = match.group(1).strip()
        logger.debug(f"[Sanitizer]   └─ Regex match FOUND — extracted {len(kql)} chars")
    else:
        kql = clean_text
        logger.warning(f"[Sanitizer]   └─ Regex match NOT FOUND — falling back to full cleaned text")

# ------------------------------------------------------------------
    # SUB-STEP 7c: Integrity check — ensure table name is present
    # ------------------------------------------------------------------
    logger.debug(f"[Sanitizer] STEP 7c — Integrity check: 'API_gateway' presence")

    if "API_gateway" not in kql:
        if kql.startswith("|"):
            logger.warning(f"[Sanitizer]   └─ Table name missing but pipe-chain detected — auto-prepending 'API_gateway'")
            kql = "API_gateway\n" + kql
        else:
            logger.error(f"[Sanitizer]   └─ INVALID KQL — 'API_gateway' not found and no pipe-chain: {kql!r}")
            return ""
    else:
        logger.debug(f"[Sanitizer]   └─ Integrity check PASSED — 'API_gateway' found in output")

    return kql