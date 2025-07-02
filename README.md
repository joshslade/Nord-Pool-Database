# Nord Pool Database Project

## Project Overview

This project is designed to automatically fetch "Day Ahead" electricity market data from the Nord Pool Data Portal API and store it in a PostgreSQL database. The data includes various metrics such as flow, capacities, price indices, prices, and volumes. The system is built to ensure data is regularly updated and transformed into a clean, relational format suitable for business intelligence (BI) and machine learning (ML) applications.

### Architecture

The core components of this project include:

*   **Data Source:** The primary data source is the [Nord Pool Data Portal API](https://www.nordpoolgroup.com/en/Market-data1/European-power-markets/Nordic/Day-ahead-prices/). Specific endpoints used are listed in `APIs.md`.
*   **Database:** PostgreSQL, with a strong emphasis on leveraging Supabase's features, including `pg_cron` for scheduling and `pg_net` for secure outbound HTTP requests directly from the database.
*   **Scheduling:** `pg_cron` is used to automate the daily data fetching and processing tasks.
*   **API Interaction:** The `pg_net` extension enables the PostgreSQL database to make direct HTTP GET requests to the Nord Pool API, retrieving JSON payloads.
*   **Data Processing:** SQL functions handle the ingestion of raw JSON data, its parsing, and subsequent transformation into structured core tables. Further SQL transformations create analytical views for consumption.
*   **Monitoring:** Github actions is used to perform a daily check and send email confirmation that the API call was successful and fresh data was added.


### Data Flow

```mermaid
sequenceDiagram
    autonumber
    participant CRON as pg_cron (Supabase)
    participant DB as PostgreSQL
    participant NET as pg_net
    participant API as External REST APIs

    Note over CRON: Every day @ 09:00 Europe/London
    CRON->>DB: CALL api_fetch_all()
    DB->>NET: SELECT pg_net.http_get(url, headers…)
    NET->>API: HTTPS GET
    API-->>NET: JSON payload
    NET->>DB: INSERT INTO _http_response(request_id,…,response_json)

    Note over CRON: 5 min safety buffer
    CRON->>DB: CALL process_responses()
    DB->>DB: 1. Copy raw → raw.http_json 2. Call *parse_* fns → core tables
    DB-->>Analytics: Clean relational data ready for BI/ML
```
The data flow within the project follows a scheduled, two-phase process:

1.  **Data Fetching (Daily @ 09:00 Europe/London):**
    *   A `pg_cron` job triggers the `api_fetch_all()` SQL function.
    *   This function utilizes `pg_net` to send HTTP GET requests to various Nord Pool Data Portal API endpoints (as defined in `APIs.md`).
    *   The API responses, in JSON format, are then inserted into a staging table, `_http_response`, within the PostgreSQL database.

2.  **Data Processing (5 minutes after fetching):**
    *   A second `pg_cron` job, scheduled with a 5-minute safety buffer, calls the `process_api_responses()` SQL function.
    *   `process_api_responses()` performs the following steps:
        *   It copies the raw JSON data from the `_http_response` staging table into a more permanent `raw.http_json` table.
        *   It then invokes a series of `*parse_*` functions (e.g., `json_parse_test.sql` demonstrates this capability) to extract relevant data points from the raw JSON.
        *   The extracted data is then inserted into the respective core tables (e.g., `core-da_capacity.sql`, `core-da_flow.sql`, `core-da_price.sql`, etc., defined in the `schemas/` directory).
        *   Finally, analytical views (e.g., defined in `analytics-view_transforms.sql`) are created or updated from these core tables, providing a clean, relational dataset ready for analysis, reporting, or machine learning model training.

### Key Project Directories and Files

*   `schemas/`: Contains SQL scripts defining the database schema, including core tables for storing parsed data and analytical views.
*   `data/samples/`: Provides sample JSON responses from the Nord Pool API, useful for testing and understanding the data structure.
*   `Scripts/`: Houses various SQL scripts for database initialization (`init.sql`), scheduling cron jobs (`schedule_cron.sql`), back-populating historical data (`back_populate_API_calls.sql`), and JSON parsing tests (`json_parse_test.sql`).
*   `APIs.md`: Lists the specific Nord Pool Data Portal API endpoints that the project interacts with.

## Setup and Installation

To get this project up and running, follow these steps:

### Prerequisites

*   **PostgreSQL Database:** A PostgreSQL database instance is required. It is highly recommended to use [Supabase](https://supabase.com/) as it provides `pg_cron` and `pg_net` extensions out-of-the-box, which are crucial for this project's automation.
*   **Python 3.11:** Ensure you have Python 3.11 installed.
*   **Conda (Recommended):** For managing Python environments and dependencies, [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or Anaconda is recommended.

### Database Configuration

1.  **Enable Extensions:**
    If you are using Supabase, `pg_net` and `pg_cron` extensions can be enabled directly from the Supabase dashboard under "Database" -> "Extensions". For other PostgreSQL installations, you might need to install them manually.

2.  **Initialize Database Schemas:**
    Connect to your PostgreSQL database and run the `init.sql` script to create the necessary schemas (`ext`, `core`, `analytics`):
    ```bash
    psql -U <your_user> -d <your_database> -f Scripts/init.sql
    ```
    Replace `<your_user>` and `<your_database>` with your PostgreSQL credentials.

3.  **Schedule Cron Jobs:**
    Run the `schedule_cron.sql` script to set up the daily data fetching and processing jobs. These jobs will automatically call the `api_fetch_all()` and `process_api_responses()` functions.
    ```bash
    psql -U <your_user> -d <your_database> -f Scripts/schedule_cron.sql
    ```

### Python Environment Setup

1.  **Create Conda Environment:**
    Navigate to the project's root directory and create a conda environment using the provided `environment.yml` file:
    ```bash
    conda env create -f environment.yml
    ```

2.  **Activate Environment:**
    Activate the newly created environment:
    ```bash
    conda activate nordpooldb-env
    ```

3.  **Environment Variables:**
    This project may use environment variables for database connection strings or API keys. Create a `.env` file in the project root directory and add your sensitive information there. For example:
    ```
    DATABASE_URL="postgresql://user:password@host:port/database"
    ```
    The `python-dotenv` package (included in `environment.yml`) will automatically load these variables.

## Usage

Once the setup is complete, the project will largely operate autonomously due to the scheduled cron jobs.

### Automated Data Fetching and Processing

*   The `pg_cron` jobs configured in `Scripts/schedule_cron.sql` will automatically fetch data from the Nord Pool API daily at 09:00 Europe/London and process it 5 minutes later.
*   You can verify the scheduled jobs by querying the `cron.job` table in your PostgreSQL database:
    ```sql
    SELECT jobid, jobname, schedule FROM cron.job ORDER BY jobname;
    ```

### Accessing Processed Data

*   The cleaned and transformed data is available in the `analytics` schema. You can query these tables directly using any PostgreSQL client or integrate them with your BI tools or machine learning workflows.
*   For example, to view the processed day-ahead prices:
    ```sql
    SELECT * FROM analytics.day_ahead_prices LIMIT 100;
    ```

### Manual Data Operations (Advanced)

*   While automated, you can manually trigger the data fetching or processing functions if needed.
    *   To manually fetch data: `SELECT ext.api_fetch_all();`
    *   To manually process responses: `SELECT ext.process_api_responses();`
*   Refer to the SQL scripts in the `Scripts/` directory for more advanced operations, such as `back_populate_API_calls.sql` for historical data ingestion.
