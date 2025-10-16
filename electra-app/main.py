from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import grid, bus, load, generator, shunt, transformer2w, line, health

app = FastAPI(
    title="Electra API",
    description="Backend API para la aplicación Electra",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(grid.router, prefix="/grid", tags=["grid"])
app.include_router(bus.router, prefix="/bus", tags=["bus"])
app.include_router(load.router, prefix="/load", tags=["load"])
app.include_router(generator.router, prefix="/generator", tags=["generator"])
app.include_router(shunt.router, prefix="/shunt", tags=["shunt"])
app.include_router(transformer2w.router, prefix="/transformer2w", tags=["transformer2w"])
app.include_router(line.router, prefix="/line", tags=["line"]) 
app.include_router(health.router, prefix="/health", tags=["health"]) 
