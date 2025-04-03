# Daily Sales Report Pipeline

This project is an **Apache Airflow** pipeline designed to process daily sales data, transform it, and generate a detailed sales report in CSV format. The pipeline uses **PostgreSQL** as the database backend and is containerized using **Podman** for easy deployment.

---

## Features
- **Database Initialization**: Creates tables for sales data and daily sales reports.
- **Data Transformation**: Processes raw sales data to calculate metrics like total sales, average price and highest-selling product.
- **Report Generation**: Generates a CSV report for the given sales date.
- **Modular Design**: Uses Airflow's `TaskGroup` for better task organization.
- **Customizable**: Accepts parameters like `sales_date` for dynamic processing.

---

## Prerequisites
1. **Podman**: Ensure Podman is installed on your system.
2. **Apache Airflow**: Installed and configured.
3. **PostgreSQL**: Installed and running on port `5432` (or configured as per your setup).

---

## Project Structure
```
.
├── dags/
│   ├── daily_sales_report.py   # Main DAG file
├── daily_sales_reports/        # Directory for generated reports
├── README.md                   # Project documentation
```

---

## Setting Up the Environment

### 1. Start PostgreSQL and Airflow Containers
Use **Podman** to start the PostgreSQL and Airflow containers.

#### Start PostgreSQL Container
```bash
podman run --name postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=your_password -e POSTGRES_DB=usermanagement -p 5432:5432 -d postgres
```

#### Start Airflow Container
```bash
podman run --name airflow -p 8080:8080 -v $(pwd)/dags:/opt/airflow/dags -e AIRFLOW__CORE__LOAD_EXAMPLES=False -d apache/airflow:2.6.0
```

### 2. Configure Airflow Connection
Update the Airflow connection to point to PostgreSQL on port `5432`.

#### Using Airflow CLI
```bash
airflow connections add 'postgres_localhost' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-port '5432' \
    --conn-schema 'airflow' \
    --conn-login 'postgres' \
    --conn-password 'your_password'
```

---

## How to Use

### 1. Access the Airflow Web UI
- Open your browser and go to `http://localhost:8080`.
- Login with the default credentials (`airflow`/`airflow`).

### 2. Trigger the DAG
- Navigate to the **DAGs** page.
- Find the `daily_sales_report` DAG and click the **Trigger DAG with config** button.
- Provide the `sales_date` parameter (e.g., `2025-04-01`).

### 3. View the Generated Report
- The report will be saved in the `daily_sales_reports/` directory as a CSV file.
- Example file name: `daily_report_2025-04-01.csv`.

---

## DAG Workflow
1. **Database Initialization**:
   - Creates `sales_data` and `daily_sales_reports` tables if they don't exist.
2. **Data Extraction**:
   - Extracts sales data for the given `sales_date`.
3. **Data Transformation and Loading**:
   - Calculates metrics like total sales, average price and highest-selling product.
   - Inserts the transformed data into the `daily_sales_reports` table.
4. **Report Generation**:
   - Exports the transformed data into a CSV file.

---

## PostgreSQL Tables

### `sales_data`
| Column Name       | Data Type       | Description                     |
|-------------------|-----------------|---------------------------------|
| `sale_id`         | SERIAL          | Primary key                     |
| `product_id`      | INTEGER         | ID of the product               |
| `product_name`    | VARCHAR(100)    | Name of the product             |
| `quantity`        | INTEGER         | Quantity sold                   |
| `price`           | DECIMAL(10, 2)  | Price of the product            |
| `sale_date`       | DATE            | Date of the sale                |

### `daily_sales_reports`
| Column Name               | Data Type       | Description                     |
|---------------------------|-----------------|---------------------------------|
| `report_date`             | DATE            | Date of the report              |
| `total_sales`             | DECIMAL(10, 2)  | Total sales amount              |
| `total_quantity`          | INTEGER         | Total quantity sold             |
| `average_price`           | NUMERIC         | Average price of products sold  |
| `number_of_transactions`  | INTEGER         | Total number of transactions    |
| `unique_products_sold`    | INTEGER         | Number of unique products sold  |
| `highest_selling_product_id` | INTEGER      | ID of the highest-selling product |
| `highest_selling_product_name` | TEXT       | Name of the highest-selling product |

---

## Troubleshooting

### PostgreSQL Connection Issues
- Ensure PostgreSQL is running on port `5432`.
- Verify the Airflow connection `postgres_localhost` is configured correctly.

### Airflow DAG Errors
- Check the Airflow logs for detailed error messages.
- Ensure the `sales_date` parameter is provided when triggering the DAG.

---

## License
This project is licensed under the MIT License.