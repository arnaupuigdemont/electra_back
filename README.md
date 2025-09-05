# electra_back

.\env_backend\Scripts\Activate.ps1

# run app in local server from electra-app

C:/Users/arnau/Documents/UNI/tfg/electra_back/env_backend/Scripts/Activate.ps1
cd electra-app
uvicorn main:app --reload

API Documentation (Swagger UI): http://127.0.0.1:8000/docs