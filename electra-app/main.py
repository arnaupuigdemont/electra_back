from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import grid, bus, load, generator, shunt, transformer2w, line, health

_app_instance: FastAPI | None = None


def get_app() -> FastAPI:
    
    # Singleton per l'aplicació FastAPI
    global _app_instance
    if _app_instance is None:
        _app_instance = FastAPI(
            title="Electra API",
            description="Backend API para la aplicación Electra",
            version="1.0.0",
        )

        _app_instance.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        _app_instance.include_router(grid.router, prefix="/grid", tags=["grid"])
        _app_instance.include_router(bus.router, prefix="/bus", tags=["bus"])
        _app_instance.include_router(load.router, prefix="/load", tags=["load"])
        _app_instance.include_router(generator.router, prefix="/generator", tags=["generator"])
        _app_instance.include_router(shunt.router, prefix="/shunt", tags=["shunt"])
        _app_instance.include_router(transformer2w.router, prefix="/transformer2w", tags=["transformer2w"])
        _app_instance.include_router(line.router, prefix="/line", tags=["line"]) 
        _app_instance.include_router(health.router, prefix="/health", tags=["health"])
    
    return _app_instance


# Crear la instancia per uvicorn
app = get_app() 




