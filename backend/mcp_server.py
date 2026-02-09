# backend/mcp_server.py
import re
import uuid
import time
from typing import Dict
import logging


logger = logging.getLogger(__name__)

class MCPServer:
    def __init__(self):
        self.allowed_table = "API_gateway"

        # ---------------------------------------------------------
        # BLOCKLIST: Security & Integrity Protection
        # ---------------------------------------------------------
        self.blocked_patterns = [
            r"^\s*\.",          # Control commands start with dot (.)
            r"\.drop\b",        # Explicit drops
            r"\.alter\b",       # Explicit alters
            r"\.create\b",      # Explicit creates
            r"\.set\b",         # Setting policies
            r"\.ingest\b",      # Ingesting data
            r";"                # Semicolon (Basic SQL Injection prevention)
        ]

    # ---------------------------
    # ENTRY POINT
    # ---------------------------
    def process(self, tool: str, kql: str, goal: str) -> Dict:
        # 1. Start Tracing
        trace_id = str(uuid.uuid4())
        start_time = time.time()

        
        # 2. LOG REQUEST
        self._log_event(trace_id, "REQUEST", {
            "tool": tool,
            "goal": goal,
            "kql": kql
        })

        try:
            if tool != "adx":
                raise ValueError("MCPServer received non-adx tool request")

            if not kql or not kql.strip():
                raise ValueError("Empty KQL is not allowed")

            # 3. Basic Cleanup
            clean_kql = kql.strip()

            # 4. Security Check (Block Dangerous Commands)
            self.validate_safety(clean_kql)

            # 5. Table Check (Must start with correct table)
            self.validate_table_access(clean_kql)

            # ---------------------------------------------------------
            # 6. ENTERPRISE SAFETY: INJECT LIMITS
            # If the user asks for "Show data" without limits, we force one.
            # This prevents 1 Billion rows from crashing the server.
            # ---------------------------------------------------------
            validated_kql = self.inject_safety_limits(clean_kql)

            # 7. Success Logging
            latency = round(time.time() - start_time, 3)
            self._log_event(trace_id, "ACCEPTED", {"latency_sec": latency})

            return {
                "trace_id": trace_id,
                "validated_kql": validated_kql
            }

        except Exception as e:
            # Error Logging
            latency = round(time.time() - start_time, 3)
            self._log_event(trace_id, "BLOCKED", {"error": str(e), "latency_sec": latency})
            raise e

    # ---------------------------
    # SECURITY LOGIC
    # ---------------------------
    def validate_safety(self, kql: str):
        """
        Block commands starting with dot (.) or containing explicit admin keywords.
        """
        if kql.startswith("."):
            raise ValueError("Security Alert: Control commands (starting with '.') are NOT allowed.")

        for pattern in self.blocked_patterns:
            if re.search(pattern, kql, re.IGNORECASE | re.MULTILINE):
                raise ValueError(f"Security Alert: Blocked pattern detected -> {pattern}")

    def validate_table_access(self, kql: str):
        """
        Ensure the query actually starts with the allowed table.
        Robustness: Handles 'let' statements and whitespace.
        """
        # We look for the table name as a distinct word boundary (\b)
        # This passes: "let x=1; StormEventsCopy | ..."
        # This passes: "StormEventsCopy | ..."
        if not re.search(r"\b" + re.escape(self.allowed_table) + r"\b", kql, re.IGNORECASE):
            raise ValueError(f"Access Denied: Query must target table '{self.allowed_table}'.")
        
    def inject_safety_limits(self, kql: str) -> str:
        """
        ENTERPRISE FEATURE:
        If the query is a raw data dump (no 'summarize', no 'count') AND lacks a limit,
        we automatically append '| take 50' to protect the backend.
        """
        lower_kql = kql.lower()
        
        # 1. Check for Aggregations (Safe)
        # We use regex \b to match whole words only.
        # Prevents "summarize_data" (var name) from bypassing the check.
        is_aggregation = re.search(r"\b(summarize|count|render)\b", lower_kql)
        
        # 2. Check for Limits (Safe)
        # Prevents "where State == 'limit'" from bypassing the check.
        has_limit = re.search(r"\b(take|limit|top)\b", lower_kql)
        
        if not is_aggregation and not has_limit:
            logger.warning("[MCP] ⚠️ Unbounded query detected. Injecting safety limit.")
            return kql + "\n| take 50"
        
        return kql

    # ---------------------------
    # LOGGING HELPER
    # ---------------------------
    def _log_event(self, trace_id: str, stage: str, data: Dict):
        """
        Internal helper to route logs to the correct level (INFO vs ERROR).
        """
        msg = f"[MCP] trace_id={trace_id} | stage={stage} | data={data}"
        
        if stage == "BLOCKED":
            logger.error(msg)
        else:
            logger.info(msg)