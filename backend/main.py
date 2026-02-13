# backend/main.py
from fastapi import FastAPI
from backend.schemas import ChatRequest
from backend.orchestrator import llm_decider
from backend.query_planner import generate_kql
from backend.adx_client import run_kql, ADXSemanticError
from backend.utils import execute_with_backoff
from backend.mcp_server import MCPServer
from backend.formatter import format_response
from backend.chat_llm import chat_llm
from fastapi.middleware.cors import CORSMiddleware

import logging
logger = logging.getLogger(__name__)


# 👇 IMPORT THE NEW CONFIG
from backend.logging_config import setup_logging

# 1. Setup Logging (First thing that happens!)
setup_logging()
app = FastAPI(title="LLM + ADX + MCP Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



mcp = MCPServer()

@app.post("/chat")
async def chat(req: ChatRequest):
    user_message = req.message.strip()

    # -----------------------
    # Step 1: Orchestrator (The Router)
    # -----------------------
    # The Orchestrator decides if the user wants to:
    # 1. Chat ("chat") -> Simple greeting
    # 2. Query Data ("adx") -> Database interaction
    # 3. Ask unrelated questions ("out_of_scope") -> Polite refusal
    decision = llm_decider(user_message)

    # ✅ LOGGING: Router Decision
    logger.info(
        f"\n================ ROUTER DECISION ================\n"
        f"User Query : {user_message}\n"
        f"Tool       : {decision.tool}\n"
        f"Query Goal : {decision.query_goal}\n"
        f"================================================"
    )

    # -----------------------
    # PATH A: Simple Chat (Greetings)
    # -----------------------
    if decision.tool == "chat":
        return {"reply": chat_llm(user_message)}

    # -----------------------
    # PATH B: Out of Scope
    # -----------------------
    if decision.tool == "out_of_scope":
        return {
            "reply": (
                "This topic extends far beyond what we’re covering here. Exploring it would involve a broader context and deeper evaluation. Let’s stay aligned with the current objectives."
            )
        }
        
    # -----------------------
    # PATH C:Column Meaning
    # -----------------------
    if decision.tool == "column_meaning":
        return {
            "reply":decision.query_goal
            }
        
    # -----------------------
    # PATH D: ADX Database Query (Enterprise Self-Healing)
    # -----------------------
    if decision.tool == "adx":
        # Guard: Check if the Orchestrator failed to extract a goal
        if not decision.query_goal.strip():
            logger.warning("⚠️ Orchestrator selected ADX but returned empty goal.")
            return {
                "reply": "I couldn't understand a clear data request. Please rephrase your question."
            }
        # =========================================================
        # 🔄 SELF-HEALING LOOP
        # Instead of trying once and failing, we try up to 3 times.
        # If an error occurs, we feed the error back to the LLM to fix it.
        # =========================================================
        MAX_RETRIES = 2
        attempt = 0
        last_semantic_error = None
        last_kql = None

        while attempt <= MAX_RETRIES:
            try:
                # -----------------------
                # Step 2: Query Generation
                # -----------------------
                # If attempt == 0: Generates fresh KQL from user goal.
                # If attempt > 0 : Uses 'Repair Mode' to fix the 'last_error'.
                logger.info(f"🔹 [Attempt {attempt}] Generating KQL...")
                
                kql = generate_kql(decision.query_goal,retry_count=attempt,last_error=last_semantic_error, last_kql=last_kql)

                if not kql:
                    # If LLM returns nothing, force a retry with a generic error
                    raise ValueError("LLM generated empty or invalid KQL syntax.")
                last_kql = kql
                # -----------------------
                # Step 3: MCP Validation (Guardrails)
                # -----------------------
                # Checks for security risks (e.g., .drop table) or missing limits.
                # If unsafe, it raises ValueError, which triggers the retry loop.
                mcp_result = mcp.process(
                    tool="adx",
                    kql=kql,
                    goal=decision.query_goal
                )
                
                validated_kql = mcp_result["validated_kql"]

                # -----------------------
                # Step 4: ADX Execution
                # -----------------------
                # Runs the query against Azure.
                # - Raises ValueError for semantic errors (e.g., "Invalid column") -> Retries
                # - Raises ConnectionError for network issues -> Stops loop
                # ⚡ AWAIT the async execution
                data = await execute_with_backoff(run_kql, validated_kql)

                # Guard: no data
                if not data:
                    logger.info("✅ Query successful but returned 0 rows.")
                    return {
                        "reply": "No data was found for your request."
                    }
                # --- SUCCESS ---
                # If we reach here, the query worked! We can break the loop.
                
                # -----------------------
                # Step 5: Data Formatting
                # -----------------------
                # We truncate data to 15 rows to prevent crashing the LLM context window
                MAX_ROWS_FOR_LLM = 100
                # Note: ADX Client already handles datetime serialization
                preview_data = data[:MAX_ROWS_FOR_LLM]
                
                system_result = {
                    "total_rows_found": len(data),
                    "rows_shown_to_ai": len(preview_data),
                    "data_sample": preview_data,
                    "note": "Data truncated for performance" if len(data) > MAX_ROWS_FOR_LLM else "Full data shown"
                }

                logger.info(f"✅ Data retrieved: {len(data)} rows found.")
                # Use the LLM to explain the data to the user
                final_answer = format_response(user_message, system_result)
                return {"reply": final_answer}

            except ADXSemanticError as e:
                # 🧠 LANE 1 ERROR: AI MISTAKE
                # The server responded "Invalid Column" or "Syntax Error".
                # We catch this and loop back so the AI can fix it.
                logger.warning(f"📉 [AI Logic Flaw] Attempt {attempt} failed: {e}")
                last_semantic_error = str(e)
                attempt += 1
                
            except Exception as e:
                # 🛑 LANE 2 ERROR: CRITICAL FAILURE
                # If we get here, it means 'execute_with_backoff' gave up after 3 network retries.
                # The AI cannot fix a down server. Stop the loop.
                logger.error(f"❌ [Critical System Failure] {str(e)}", exc_info=True)
                return {"reply": "I encountered a system error connecting to the database."}


        # -----------------------
        # FAILURE (Loop Exhausted)
        # -----------------------
        # If we exit the while loop, it means we tried 3 times (0, 1, 2) and failed every time.
        logger.error(f"❌ All {MAX_RETRIES + 1} attempts failed. Giving up.")
        return {
            "reply": (
                "I tried to run the query multiple times, but I kept encountering technical errors. "
                "Please try rephrasing your request."
            )
        }
    