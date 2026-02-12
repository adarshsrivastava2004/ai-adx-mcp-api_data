# backend/schemas.py
from pydantic import BaseModel, Field
from typing import Literal


class ChatRequest(BaseModel):
    """
    Raw user input received from API / UI.
    """
    message: str = Field(
        ...,
        min_length=1,
        description="Raw user message"
    )


class ToolDecision(BaseModel):
    """
    Output of the LLM router.

    FIXED (v1):
    Tool meanings are now STRICT and non-overlapping.

    - chat
        → ONLY greetings like: hi, hello, hey
        → Response is generated directly by LLM (no formatter, no JSON)

    - adx
        → Database-related analytical queries
        → query_goal MUST be non-empty
        → Goes through planner → MCP → ADX → formatter
    - column_meaning (NEW)
        → Questions about specific columns (e.g. "What is apiStatusCode?")
        → query_goal contains the explanation text
        
    - out_of_scope
        → Meaningful non-database questions (definitions, explanations, general knowledge)
        → query_goal MUST be empty
        → Response is generated via formatter LLM using (query + JSON)
    """

    tool: Literal["chat", "adx", "out_of_scope", "column_meaning"] = Field(
        ...,
        description="Routing decision selected by the orchestrator"
    )

    query_goal: str = Field(
        ...,
        description=(
            "Clean query intent for ADX execution. "
            "This MUST be non-empty ONLY when tool == 'adx'. "
            "It MUST be an empty string for 'chat' and 'out_of_scope'."
        )
    )
