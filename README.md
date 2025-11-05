# Airflow Project

This project provides a complete Apache Airflow setup using Docker Compose for orchestrating and scheduling data workflows.

## 📋 About the Project

This is a containerized Apache Airflow environment that includes:
- **Airflow Webserver**: Web UI for managing DAGs and monitoring workflows
- **Airflow Scheduler**: Schedules and manages task execution
- **Airflow Worker**: Executes tasks using Celery executor
- **Redis**: Message broker for Celery
- **Custom DAGs**: Sample workflows demonstrating various Airflow features

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone git@github.com:uniacco-tech/airflow.git
cd airflow
```

## 🔄 DAG Merge and Deployment Process

### For Contributing DAGs to Main Branch

1. **Create Feature Branch**
   ```bash
   git checkout -b feature/new-dag-name
   ```

2. **Develop Your DAG**
   - Add your DAG file to `dags/` directory
   - Test locally using the development setup
   - Ensure DAG follows naming conventions and best practices

4. **Create Pull Request**
   - Push your feature branch to remote repository
   - Create a pull request against the `main` branch
   - Include description of what the DAG does
   - Add any necessary documentation updates

5. **Code Review Process**
   - Ensure DAG follows project standards
   - Verify proper error handling and logging
   - Check for security considerations
   - Validate DAG metadata (tags, description, schedule)

6. **Merge to Main**
   - After approval, merge the pull request
   - DAG will be automatically available in production environment
   - Monitor DAG execution in Airflow UI

### Deployment Steps
1. **Automatic Deployment**: DAGs in the main branch are automatically synced to production
2. **Manual Verification**: Check Airflow UI to ensure DAG appears correctly
3. **Enable DAG**: Unpause the DAG in the web interface if needed
4. **Monitor**: Watch initial runs for any issues

## 🎓 Local Setup

### Prerequisites

Before starting, ensure you have the following installed:
- **Docker**: [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose**: Usually included with Docker Desktop

### Local Environment Setup

1. **Setup PostgreSQL Database**
   
   For local development, we need Postgres:

   **Local PostgreSQL Installation**
   
   If you have PostgreSQL installed locally:
   ```bash
   # Install PostgreSQL (macOS)
   brew install postgresql
   brew services start postgresql
   
   # Create database and user
   createdb airflow
   psql -c "CREATE USER airflow WITH PASSWORD 'airflow';"
   psql -c "GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;"
   ```

3. **Start Services**
   ```bash
   # Start PostgreSQL first
   docker compose -f docker-compose.yaml up -d
   
   # Wait for PostgreSQL to be ready
   sleep 10
   
   # Initialize Airflow database (first time only)
   docker-compose up airflow-init
   
   # Start all Airflow services
   docker-compose up -d
   ```

3. **Access Airflow Web UI**
   - Open your browser and go to: http://localhost:8080
   - Default credentials:
     - Username: `airflow`
     - Password: `airflow`

### Environment Configuration Management

You can easily switch between different database configurations by modifying the `.env` file:

**For Local Development with Docker PostgreSQL:**
```bash
# In .env file
DB_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
```

**For Local Development with Host PostgreSQL:**
```bash
# In .env file  
DB_CONN=postgresql+psycopg2://airflow:airflow@host.docker.internal:5432/airflow
```

After changing database configuration:
```bash
# Restart services to pick up new configuration
docker-compose down
docker-compose up airflow-init  # Re-initialize if switching databases
docker-compose up -d
```

### Local Development Tips

- **Auto-reload DAGs**: Changes to DAG files are automatically detected
- **Test DAGs**: Use `docker-compose exec airflow-webserver airflow dags test <dag_id> <execution_date>`
- **Debug with logs**: Check task logs in the web UI for troubleshooting
- **Use DAG validation**: Run `python dags/your_dag.py` to check syntax
- **Environment isolation**: Each DAG runs in its own environment

### Common Learning Exercises

1. **Schedule Variations**: Create DAGs with different schedules (hourly, daily, weekly)
2. **Error Handling**: Intentionally create failing tasks to understand retries and alerts
3. **Data Pipeline**: Build a simple ETL pipeline using multiple operators
4. **External Integration**: Connect to databases, APIs, or cloud services
5. **Monitoring**: Set up alerts and monitoring for your workflows

### Stopping the Environment

When you're done learning:
```bash
# Stop all Airflow services
docker-compose down

# Stop PostgreSQL (if using Docker option)
docker-compose -f docker-compose.local.yaml down

# Remove volumes (careful: this deletes all data including database)
docker-compose down -v
docker-compose -f docker-compose.local.yaml down -v

# Or stop everything including volumes in one command
docker-compose down -v && docker-compose -f docker-compose.local.yaml down -v
```

## 🔧 Development Workflow

### DAG Development Best Practices

- Place DAGs in the `dags/` folder
- Use meaningful DAG IDs and descriptions
- Add appropriate tags for organization
- Set proper start dates and schedules
- Include error handling and retries
- Use task groups for complex workflows


## 📁 Project Structure

```
airflow/
├── docker-compose.yaml     # Docker Compose configuration
├── Dockerfile             # Custom Airflow image (if needed)
├── .env                   # Environment variables
├── README.md              # This file
├── dags/                  # Airflow DAGs directory
│   ├── pilot_test_dag.py  # Sample test DAG
│   └── UI_features_toy.py # UI features demonstration DAG
├── logs/                  # Airflow logs
├── plugins/               # Custom Airflow plugins
└── config/                # Additional configuration files
```

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Docker Guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [DAG Writing Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

