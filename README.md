# Pay-As-You-Go Engine (Airflow Middleware)

This project is a specialized data orchestration middleware built on **Apache Airflow 3.0.3**. It is designed to handle mass data synchronization between **PrestaShop** and **Odoo** using a "Pay-As-You-Go" consumption model.

## Features
- **Golden Sync Pattern**: Unified DAG for all component types (Products, Categories, Customers, etc.).
- **Centralized Billing Integration**: Real-time credit checking and deduction via a dedicated Billing Server.
- **Bidirectional Traceability**: Maintains external ID bindings in Odoo.
- **Optimized for Mass Data**: Uses batch processing and incremental updates.

## Prerequisites
- **Python**: 3.12 or higher.
- **Database**: PostgreSQL (recommended for production) or SQLite (for testing).
- **Middleware Core**: `innovation-connector-core` package must be accessible in the Python path.

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/arkigen/pay-as-you-go-engine.git
   cd pay-as-you-go-engine
   ```

2. **Setup Virtual Environment**:
   ```bash
   python3.12 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Initialize Airflow**:
   ```bash
   export AIRFLOW_HOME=$(pwd)
   airflow db init
   ```

## Configuration

The main configuration is in `airflow.cfg`. Key sections:

- `[core]`: Ensure `dags_folder` and `plugins_folder` point to the correct absolute paths.
- `[core] auth_manager`: Set to `SimpleAuthManager` for lightweight API/UI security.
- `[core] simple_auth_manager_users`: Define your admin user (e.g., `admin:admin`).

### Environment Variables
For production, you can override settings using environment variables:
- `AIRFLOW__CORE__EXECUTOR=LocalExecutor`
- `AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:pass@host/db`

## Deployment

### 1. Manual Start (Testing)
Use the included helper script to start the API server, Scheduler, and DAG Processor:
```bash
./start-airflow-ui.sh
```

### 2. Systemd (Production)
Create systemd units for `airflow-api-server`, `airflow-scheduler`, and `airflow-dag-processor`.

Example `airflow-scheduler.service`:
```ini
[Unit]
Description=Airflow Scheduler
After=network.target postgresql.service

[Service]
User=airflow
Group=airflow
Type=simple
Environment="AIRFLOW_HOME=/opt/airflow"
ExecStart=/opt/airflow/.venv/bin/airflow scheduler
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Usage

### API Authentication
The middleware uses **SimpleAuthManager**. On the first start, it generates a password file:
`simple_auth_manager_passwords.json.generated`

Use these credentials to obtain a JWT token for API calls:
`POST /auth/token` with `{"username": "admin", "password": "..."}`

### Triggering Synchronization
The `golden_sync` DAG is triggered via the Airflow API. It expects a JSON configuration:

```json
{
  "conf": {
    "component_type": "product",
    "direction": "prestashop_to_odoo",
    "ps_config": {
      "url": "https://your-prestashop.com",
      "api_key": "YOUR_PS_KEY"
    },
    "odoo_config": {
      "url": "http://your-odoo.com",
      "db": "your_db",
      "username": "admin",
      "password": "..."
    },
    "billing_url": "http://billing-server.com",
    "billing_api_key": "YOUR_BILLING_KEY",
    "platform_hash": "UNIQUE_HASH"
  }
}
```

## Integration with Billing
The engine automatically checks for credits before starting a sync. If `billing_url` is provided, it will:
1.  **Pre-check**: Verify `remaining_credits > 0`.
2.  **Sync**: Process records up to the available credit limit.
3.  **Report**: Post the `updated_count` back to the Billing Server to deduct credits.

## Remote Deployment

To move this engine to a remote server, follow these steps:

1. **Prepare the Package**:
   Download the `airflow-connector.zip` file containing the core engine components.

2. **Transfer and Extract**:
   ```bash
   scp airflow-connector.zip user@remote-server:/opt/
   ssh user@remote-server
   cd /opt/
   unzip airflow-connector.zip -d airflow-engine
   cd airflow-engine
   ```

3. **Install Dependencies**:
   Ensure Python 3.12+ is installed, then:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   
   # 1. Install the core connector package (REQUIRED)
   # If you have the source folder:
   cd innovation-connector-core
   pip install -e .
   cd ..
   
   # 2. Install Airflow and other dependencies
   pip install -r requirements.txt
   ```

4. **Initialize Database**:
   For production, it is highly recommended to use PostgreSQL. Update `AIRFLOW__CORE__SQL_ALCHEMY_CONN` in your environment or `airflow.cfg` before running:
   ```bash
   export AIRFLOW_HOME=$(pwd)
   airflow db init
   ```

5. **Configure Paths**:
   Update `airflow.cfg` to ensure `dags_folder` and `plugins_folder` point to the new absolute paths on the remote server.

6. **Start Services**:
   Use the `start-airflow-ui.sh` script or set up systemd units as described in the "Deployment" section above.

## Project Structure
- `/dags`: Contains `golden_sync.py` (The main orchestration logic).
- `/plugins`: Custom Airflow hooks and operators for PrestaShop/Odoo.
- `airflow.cfg`: Airflow configuration file.
- `start-airflow-ui.sh`: Deployment helper script.
