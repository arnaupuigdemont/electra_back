from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from routes import gridcal
import uvicorn

# Crear instancia de FastAPI
app = FastAPI(
    title="Electra API",
    description="Backend API para la aplicación Electra",
    version="1.0.0"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especifica los dominios permitidos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(gridcal.router, prefix="/gridcal", tags=["GridCal"])

# Ejecutar el servidor si se ejecuta directamente
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="127.0.0.0",
        port=8000,
        reload=True
    )