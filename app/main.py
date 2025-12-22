from fastapi import FastAPI
from app.api import routes

app = FastAPI(title="Download Manager API")

# Include API routes
app.include_router(routes.router)

@app.get("/health")
def health_check():
    return {"status": "ok"}
