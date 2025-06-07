from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from core.config import get_settings
from apps.routes import ingestion  
from apps.routes import blueprint

settings = get_settings()

app = FastAPI(
    title=settings.APP_NAME,
    debug=settings.DEBUG,
    version="1.0.0"
)

# Optional: Configure CORS if you have a frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#Routes Inclusions
app.include_router(ingestion.router)
app.include_router(blueprint.router)

# Health check route
@app.get("/health")
async def health_check():
    return {"status": "ok", "app": settings.APP_NAME}

