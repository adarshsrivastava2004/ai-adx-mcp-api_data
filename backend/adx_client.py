# backend/adx_client.py

import logging
from typing import List, Dict, Any

# ---------------------------------------------------------
# FIXED IMPORTS
# ---------------------------------------------------------
# 1. Async Client comes from .aio
from azure.kusto.data.aio import KustoClient

# 2. Connection Builder is ALWAYS sync (it just builds a string)
from azure.kusto.data import KustoConnectionStringBuilder

# 3. Exceptions
from azure.kusto.data.exceptions import KustoServiceError, KustoClientError

from backend.config import (
    ADX_CLUSTER_URL,
    ADX_DATABASE,
    AZURE_CLIENT_ID,
    AZURE_CLIENT_SECRET,
    AZURE_TENANT_ID
)

logger = logging.getLogger(__name__)

# =========================================================
# 1. DEFINE CUSTOM EXCEPTIONS (The "Traffic Signals")
# =========================================================

class ADXSemanticError(Exception):
    """
    Raised when the ERROR IS THE AI'S FAULT.
    Examples: "Table not found", "Invalid Column", "Syntax Error".
    Action: The Main Loop should catch this and ask the LLM to fix the query.
    """
    pass

class ADXSystemError(Exception):
    """
    Raised when the ERROR IS THE INFRASTRUCTURE'S FAULT.
    Examples: "Network Down", "Auth Token Expired", "DNS Failure".
    Action: The System should retry with backoff (wait 1s, 2s, 4s). DO NOT wake the LLM.
    """
    pass


# =========================================================
# 2. MANAGER CLASS
# =========================================================

class ADXManager:
    def __init__(self):
        # We hold the client in a variable but START as None.
        # This is "Lazy Loading". We won't connect until the first query runs.
        self._client = None

    async def _get_client(self) -> KustoClient:
        """
        Retrieves the existing client or creates a new one if it doesn't exist.
        Reason: Prevents the backend from crashing immediately on startup if 
        internet is down or credentials are wrong.
        """
        if self._client:
            return self._client
        
        try:
            # Check for Service Principal (Production) vs Device Login (Local Dev)
            if AZURE_CLIENT_ID and AZURE_CLIENT_SECRET:
                kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                    ADX_CLUSTER_URL, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
                )
            else:
                kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(ADX_CLUSTER_URL)
            
            self._client = KustoClient(kcsb)
            return self._client

        except Exception as e:
            # Critical error: We cannot even create the client object.
            logger.critical(f"[AUTH FAILED] {str(e)}")
            raise ADXSystemError(f"Authentication Failed: {str(e)}")

    async def run_kql(self, query: str) -> List[Dict[str, Any]]:
        """
        Executes a KQL query and returns a clean list of dictionaries.
        """
        # We await the client getter (good practice for async init patterns)
        client = await self._get_client()
        try:
            # ⚡ KEY CHANGE: 'await' the network call.
            # This releases the server to help other users while waiting for data.
            response = await client.execute(ADX_DATABASE, query)
            
            # Kusto returns multiple tables; we only want the first one (primary results)
            if not response.primary_results:
                return []

            # Serialize results to ensure they are JSON safe (handling Dates)
            results = []
            for row in response.primary_results[0]:
                results.append(self._serialize(row.to_dict()))
            return results
        
        # -------------------------------------------------------
        # ERROR HANDLING (Same logic, just adapted for async flow)
        # -------------------------------------------------------
        except KustoServiceError as e:
            # Sometimes the SDK wraps network errors in KustoServiceError.
            # We must look at the actual message to decide.
            error_str = str(e).lower()
            
            # KEYWORDS THAT INDICATE IT IS ACTUALLY A SYSTEM ERROR:
            system_keywords = [
                "failed to process network request", 
                "connection refused", 
                "timeout", 
                "max retries exceeded", 
                "endpoint unreachable"
            ]
            
            if any(kw in error_str for kw in system_keywords):
                logger.error(f"[ADX System Error (Redirected)]: {e}")
                # Redirect to System Lane (Retry with Backoff)
                raise ADXSystemError(str(e))

            # If no network keywords found, it is a genuine Logic/Syntax error.
            logger.warning(f"[ADX Logic Error]: {e}")
            raise ADXSemanticError(str(e))
            
        except Exception as e:
            # Catch-all for other crashes (DNS, Auth, etc.)
            logger.error(f"[ADX System Error]: {e}")
            raise ADXSystemError(str(e))
    async def close(self):
        """Cleanup: Close the async session when app shuts down"""
        if self._client:
            await self._client.close()
            
    def _serialize(self, row: Dict) -> Dict:
        """
        Helper: Converts Python datetime objects to ISO strings (e.g., "2023-01-01").
        Why? FastAPI/JSON cannot send raw Python datetime objects to the frontend.
        """
        clean = {}
        for k, v in row.items():
            if hasattr(v, 'isoformat'):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        return clean

# Singleton Instance: We create one manager for the whole app
adx_manager = ADXManager()

# Public function used by main.py
async def run_kql(query: str):
    return await adx_manager.run_kql(query)