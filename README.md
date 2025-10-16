# electra_back

.\env_backend\Scripts\Activate.ps1

# run app in local server from electra-app

C:/Users/arnau/Documents/UNI/tfg/electra_back/env_backend/Scripts/Activate.ps1
cd electra-app
uvicorn main:app --reload

API Documentation (Swagger UI): http://127.0.0.1:8000/docs

## Database configuration

The backend reads `DATABASE_URL` from environment (via `.env` in `electra-app/` or OS env).

- Recommended (host dev):
	- `postgresql://electra:electra@localhost:5432/electra`
- If backend runs in Docker network:
	- `postgresql://electra:electra@db:5432/electra`

You can create `electra-app/.env` by copying `.env.example`:

```
cp electra-app/.env.example electra-app/.env
# then edit electra-app/.env if needed
```

Note: If you previously used an SQLAlchemy-style URL like `postgresql+psycopg://...`, the backend now normalizes it automatically, but using the plain `postgresql://` form is preferred.

## Endpoints

- Upload grid file
	- POST `/grid/files/upload` (multipart form-data with `file`)
	- Rejects empty files with 400
- List grid ids
	- GET `/grid/ids`
- Delete grid (cascade + tmp file cleanup)
	- DELETE `/grid/{grid_id}`

Per-component (by primary key id):
- GET `/bus/{id}`
- GET `/load/{id}`
- GET `/generator/{id}`
- GET `/shunt/{id}`
- GET `/transformer2w/{id}`
- GET `/line/{id}`