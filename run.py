# run.py — entry point for Railway
import uvicorn
import sys
import os

# Add repo root for v2 modules (signals/, forecast/, Venue/)
sys.path.insert(0, os.path.dirname(__file__))
# Add backend for v1 modules (config, core/, models/, data/)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info",
    )
