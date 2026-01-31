# ELECTRA: Power System Analysis Engine (Backend)

## 🎓 Project Context
This project serves as the backend component for **ELECTRA**, my **Bachelor's Thesis (TFG)** in **Computer Engineering** at the **Barcelona School of Informatics (FIB - UPC)**.

It acts as the computational engine for the Electra platform, bridging the gap between modern web interfaces and rigorous power system analysis tools.

## 📋 Description
**Electra Backend** is a high-performance REST API designed to process, simulate, and manage electric grid data. It is built with **FastAPI** and uses **[VeraGridEngine](https://pypi.org/project/VeraGridEngine/)** as its scientific core to perform numerical calculations.

The API exposes endpoints to parse grid files, persist grid data in a **PostgreSQL** database, and execute complex simulations like **Power Flow**, returning the results to the frontend in real-time.

### Key Features
* **Scientific Core:** Leverages **VeraGridEngine** for high-precision power system modelling and simulation.
* **Grid Management:** Supports uploading and parsing grid files, with granular access to elements (Buses, Lines, Generators, Shunts, Transformers).
* **Simulation Engine:**
    * **Power Flow:** Calculates voltages, angles, and branch loadings.
* **Persistence:** Uses **PostgreSQL** to store grid models and simulation states.
* **Modern API:** Fully typed, async-ready endpoints documented automatically with Swagger/OpenAPI.

## 🛠️ Technical Implementation

### Core Stack
* **Language:** Python 3.12.
* **Framework:** [FastAPI](https://fastapi.tiangolo.com/) + Uvicorn.
* **Simulation Engine:** VeraGridEngine.
* **Database:** PostgreSQL (via `psycopg2-binary`).
* **Containerization:** Docker & Docker Compose.

### API Architecture
The backend is structured around resources corresponding to physical grid elements, with dedicated routers for each:
* `/grid`: File upload, simulation triggers (Power Flow), and ID listing.
* `/bus`, `/line`, `/generator`, `/load`, `/shunt`, `/transformer2w`: CRUD access to topology.
* `/health`: System health checks.

## 🚀 Usage

### Prerequisites
* Docker & Docker Compose

### Running with Docker (Recommended)
The infrastructure is defined in the `infra` folder.

1.  **Navigate to the infrastructure directory:**
    ```bash
    cd infra
    ```

2.  **Start the services (Database + API):**
    ```bash
    docker-compose up --build
    ```
    This will start a PostgreSQL container (`db`) and the API container (`backend`).

The API will be available at `http://localhost:8000`.
**Interactive Docs (Swagger UI):** `http://localhost:8000/docs`

### Local Development

If you wish to run the Python app outside of Docker (e.g., for debugging), ensure you have a PostgreSQL instance running and configured in your environment variables.

1.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Run the server:**
    ```bash
    cd electra-app
    uvicorn main:app --reload --port 8000
    ```
