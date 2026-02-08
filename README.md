# Open Interest Service

A lightweight, Dockerized microservice that fetches **Open Interest (OI)** and **Funding Rates** from [Hyperliquid](https://hyperliquid.xyz/) in real-time. It stores the data in a local SQLite database and provides a REST API for easy retrieval.

## Features

- **Real-time Data**: Fetches data every hour (at minute 0) for a configured list of trading pairs.
- **Historical Data**: Stores data with UTC timestamps for historical analysis.
- **REST API**: Simple endpoints to query current or past data.
- **Dockerized**: specific `Dockerfile` and `docker-compose.yml` for easy deployment.
- **Database**: Uses `SQLite` (async) for zero-conf persistence.

## 🚀 Installation & Running

### Option 1: Docker (Recommended)

Ensure you have Docker and Docker Compose installed.

1.  **Build and Start**:
    ```bash
    docker-compose up --build -d
    ```
    This will start the service on port `9123`. The database will persist in the local `./data` directory.

2.  **Stop**:
    ```bash
    docker-compose down
    ```

### Option 2: Local Python Environment

Requires Python 3.10+.

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Run the Service**:
    You can use the provided batch script (Windows):
    ```powershell
    .\run_local.bat
    ```
    Or run manually:
    ```bash
    mkdir data
    python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
    ```

### Option 3: Systemd Service (Ubuntu/Linux)

A `open-interest.service` file is included for running the application as a background service.

1.  **Edit the Service File**:
    Open `open-interest.service` and update the `User`, `Group`, `WorkingDirectory`, and `ExecStart` paths to match your environment.

2.  **Install the Service**:
    ```bash
    sudo cp open-interest.service /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable open-interest
    sudo systemctl start open-interest
    ```
## 📡 API Documentation

Once the service is running, you can access the interactive API docs at:
- **Swagger UI**: [http://localhost:9123/docs](http://localhost:9123/docs)
- **ReDoc**: [http://localhost:9123/redoc](http://localhost:9123/redoc)

### Endpoints

#### 1. Get List of Monitored Pairs
Returns the list of pairs currently being fetched.

- **URL**: `/pairs`
- **Method**: `GET`
- **Example**:
    ```bash
    curl http://localhost:9123/pairs
    ```

#### 2. Get Data for a Pair
Retrieve Open Interest and Funding Rate for a specific pair.

- **URL**: `/data`
- **Method**: `GET`
- **Query Parameters (Required)**:
    - `pair`: The pair string (e.g., `BTC/USDC:USDC`)
- **Query Parameters (Optional)**:
    - `timestamp`: Get data for a specific UTC ISO timestamp (e.g., `2024-05-20T10:00:00`).
    - `limit`: Return the last `N` records (candles).
- **Examples**:

    *Get latest data point:*
    ```bash
    curl "http://localhost:9123/data?pair=BTC/USDC:USDC"
    ```

    *Get last 24 hours of data:*
    ```bash
    curl "http://localhost:9123/data?pair=BTC/USDC:USDC&limit=24"
    ```

    *Get specific timestamp:*
    ```bash
    curl "http://localhost:9123/data?pair=BTC/USDC:USDC&timestamp=2024-05-20T12:00:00"
    ```

#### 3. Manually Trigger Fetch
Force the service to fetch data immediately (useful for testing or initial population).

- **URL**: `/trigger_fetch`
- **Method**: `POST`
- **Example**:
    ```bash
    curl -X POST http://localhost:9123/trigger_fetch
    ```

## ⚙️ Configuration

The list of pairs to fetch is defined in `fetcher.py`:

```python
PAIRS_CONFIG = [
    "AAVE/USDC:USDC",
    "ARB/USDC:USDC",
    "BTC/USDC:USDC",
    # ... add more pairs here
]
```

To add new pairs, update this list and restart the service.

## 💾 Database

Data is stored in a SQLite database file located at:
- **Docker**: `/app/data/open_interest.db` (mapped to `./data/open_interest.db` on host)
- **Local**: `./data/open_interest.db`

The table `oi_funding` contains:
- `id`: Primary Key
- `pair`: Pair name
- `timestamp`: UTC Timestamp
- `open_interest`: Open Interest value
- `funding_rate`: Funding Rate value
