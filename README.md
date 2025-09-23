# Cricket Database System

A production-grade, headless cricket database system with comprehensive ETL pipeline for cricket data collection, processing, and analysis.

## 🏏 Features

- **Comprehensive Data Collection**: Scrapes cricket data from multiple sources (ESPN Cricinfo, Cricket API)
- **Robust ETL Pipeline**: Extract, Transform, Load with idempotent upserts and data quality checks
- **Production-Ready Database**: MySQL 8 with optimized schema, constraints, and indexes
- **Advanced Analytics**: Rich SQL query library for cricket statistics and insights
- **CLI Interface**: Command-line tools with dry-run, rate limiting, and scheduling
- **Data Quality**: Automated validation, deduplication, and quality monitoring
- **Scalable Architecture**: Modular design with proper separation of concerns

## ⚖️ Legal Notice & Terms of Service Respect

**IMPORTANT**: This system is designed for educational and research purposes. Users must:

- **Respect robots.txt**: The system includes rate limiting and respects website crawling policies
- **Follow ToS**: Always review and comply with the Terms of Service of data sources
- **Rate Limiting**: Built-in delays and request limits to avoid overwhelming servers
- **Ethical Usage**: Use responsibly and consider the impact on source websites
- **Attribution**: Properly attribute data sources when using scraped information

The system includes configurable rate limiting, retry mechanisms, and dry-run modes to ensure respectful data collection practices.

**CRITICAL NOTES**:
- CricketArchive may require logged-in sessions and restrict automated access
- Keep credentials external (never hardcode), stop immediately if blocked
- Consider Cricsheet CSV as a lawful, robust supplementary source
- Schema supports multi-source ingest via SOURCE_ID for different data providers

## 🛡️ Compliance & Security
## ▶️ End-to-End Quickstart (copy/paste)

The following commands bootstrap the project, setup local MySQL, install Playwright, export env vars, run a dry-run refresh, apply migrations, run ETL, and QA checks.

```bash
make bootstrap
make setup-mysql
playwright install --with-deps
export $(grep -v '^#' .env | xargs) && \
  python -m etl.cli discover-latest --since 2024-01-01 && \
  python -m etl.cli refresh --since 2024-01-01 --dry-run
make migrate
make etl
make qa
```

Notes:
- `make setup-mysql` provides instructions for local MySQL setup
- On Windows PowerShell, replace the `export $(...)` line with manually setting env vars or run inside Git Bash.

### Local MySQL Setup

1. **Install MySQL 8.0+** locally on your system
2. **Run the setup script**:
   ```bash
   # Windows
   scripts\setup_mysql.bat
   
   # Linux/macOS
   ./scripts/setup_mysql.sh
   
   # Or manually
   mysql -u root -p < scripts/setup_mysql.sql
   ```
3. **Update `.env`** with your MySQL credentials:
   ```env
   DB_HOST=127.0.0.1
   DB_PORT=3306
   DB_NAME=cricket_db
   DB_USER=cricket_user
   DB_PASSWORD=cricket_password
   ```

### Database Administration

Use any MySQL client (MySQL Workbench, phpMyAdmin, or command line):
- Host: 127.0.0.1
- Port: 3306
- Username: from `.env` `DB_USER`
- Password: from `.env` `DB_PASSWORD`
- Database: from `.env` `DB_NAME`

### Example verification queries

Run these in your MySQL client to verify data presence after ETL:

```sql
-- Counts
SELECT COUNT(*) AS matches FROM matches;
SELECT COUNT(*) AS innings FROM innings;
SELECT COUNT(*) AS deliveries FROM deliveries;

-- Recent matches
SELECT id, format, start_date, venue_id, series_id, result_type, winner_team_id
FROM matches
ORDER BY start_date DESC
LIMIT 10;

-- Top batters by runs (sample)
SELECT p.full_name, SUM(bi.runs) AS runs
FROM batting_innings bi
JOIN players p ON p.id = bi.player_id
GROUP BY p.id
ORDER BY runs DESC
LIMIT 10;

-- Bowler wickets (sample)
SELECT p.full_name, SUM(bo.wickets) AS wkts
FROM bowling_innings bo
JOIN players p ON p.id = bo.player_id
GROUP BY p.id
ORDER BY wkts DESC
LIMIT 10;
```


- Respect Terms of Service and robots.txt. Only fetch content that is permitted and for which you have a legitimate use. When in doubt, obtain written consent.
- Conservative defaults to minimize load:
  - Rate limit: 1 request/second by default (configurable via `RATE_LIMIT_RPS`).
  - Exponential backoff with jitter on retries.
  - Safety cap for new work per run via `--max-new-matches` (and `MAX_NEW_MATCHES` in `.env`).
- Allowlist/Blocklist controls:
  - Configure `ETL_ALLOWLIST` and `ETL_BLOCKLIST` (comma-separated regex patterns) to explicitly permit or deny URL patterns.
  - Blocklist rules take precedence; allowlist must match when present.
- Credentials and secrets:
  - Use `.env` for local development. Never commit real secrets to source control.
  - Environment variables are loaded at runtime; rotate credentials periodically.
- Safe fetch modes:
  - `--dry-run`: execute without persisting responses to the database.
  - `--headers-only`: perform HTTP HEAD requests only, avoiding download of response bodies when you only need metadata/validation.


## 📊 Database Schema

The system includes comprehensive tables for:

- **Teams**: International and domestic cricket teams
- **Players**: Player profiles, roles, and career information
- **Matches**: Match details, venues, series, and results
- **Innings**: Inning-by-inning scorecards and statistics
- **Ball-by-Ball**: Detailed ball-by-ball data for analysis
- **Player Statistics**: Match-wise and career statistics

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- MySQL 8.0+
- Node.js (for Playwright)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd cricket-database
   ```

2. **Bootstrap the project**
   ```bash
   make bootstrap
   ```

3. **Configure environment**
   ```bash
   cp env.example .env
   # Edit .env with your database credentials
   ```

4. **Setup database**
   ```bash
   make migrate
   ```

### Basic Usage

1. **Run ETL pipeline**
   ```bash
   make etl
   ```

2. **Check data quality**
   ```bash
   make qa
   ```

3. **Run tests**
   ```bash
   make test
   ```

### Development Workflow

```bash
# Format code
make format

# Run linting
make lint

# Run tests
make test

# Run ETL pipeline
make etl

# Run quality checks
make qa

# Clean up
make clean
```

### Nightly Cron Example

Use your system scheduler to run ETL and refresh season summaries nightly. Example crontab (Linux):

```cron
# Run nightly at 02:30. Ensure virtualenv and env vars are loaded accordingly.
30 2 * * * cd /path/to/repo && SEASON_ID=123 make nightly >> logs/nightly.log 2>&1
```

On Windows Task Scheduler, create a task running:

```powershell
cmd /c "cd /d C:\path\to\repo && set SEASON_ID=123 && make nightly"
```

### ETL CLI (incremental strategy)

Top-level ETL CLI is in `src/etl/cli.py`:

```bash
# Discover latest series/competitions since a date
python -m etl.cli discover-latest --since 2024-01-01

# Fetch queued pages
python -m etl.cli fetch

# Parse recent raw_html to cached models (data/cache/parsed)
python -m etl.cli parse

# Load cached models into DB (idempotent)
python -m etl.cli load

# Run the full incremental refresh
python -m etl.cli refresh --since 2024-01-01
```

To schedule nightly refresh via cron without Docker:

```cron
0 3 * * * cd /path/to/repo && /path/to/venv/bin/python -m etl.cli refresh --since $(date -d 'yesterday' +\%Y-\%m-\%d) >> logs/etl_refresh.log 2>&1
```

## 🛠️ CLI Commands

### Database Management
```bash
# Setup database schema
python -m src.cli setup-db

# Setup with force recreation
python -m src.cli setup-db --force
```

### Data Scraping
```bash
# Scrape all data
python -m src.cli scrape

# Scrape specific data type
python -m src.cli scrape --data-type matches

# Scrape from specific source
python -m src.cli scrape --source espn

# Limit number of records
python -m src.cli scrape --limit 100
```

### Data Updates
```bash
# Incremental update (last 7 days)
python -m src.cli update --incremental

# Incremental update with custom days
python -m src.cli update --incremental --days-back 14

# Full data refresh
python -m src.cli update --full
```

### Quality & Monitoring
```bash
# Run data quality checks
python -m src.cli quality-check

# Validate data sources
python -m src.cli validate-sources

# Show system status
python -m src.cli status
```

### Scheduling
```bash
# Schedule daily updates at 2 AM
python -m src.cli schedule

# Schedule every 6 hours
python -m src.cli schedule --schedule "0 */6 * * *"

# Schedule incremental updates
python -m src.cli schedule --incremental
```

### Dry Run Mode
```bash
# Test without making changes
python -m src.cli scrape --dry-run
python -m src.cli update --incremental --dry-run
```

## 📈 SQL Query Library

The system includes a comprehensive library of SQL queries for cricket analytics:

### Batting Queries
- Top batsmen by runs, average, strike rate
- Centuries and fifties analysis
- Boundary hitting patterns
- Venue and opposition performance
- Form analysis and consistency metrics

### Bowling Queries
- Top bowlers by wickets, average, economy
- Five-wicket hauls and best figures
- Death overs and powerplay analysis
- Venue and opposition performance
- Consistency and impact analysis

### Team Queries
- Win-loss records and rankings
- Home vs away performance
- Series and tournament analysis
- Head-to-head records
- Toss impact analysis

### Match Queries
- Recent and upcoming matches
- High/low scoring matches
- Close matches analysis
- Venue characteristics
- Match momentum analysis

### Advanced Analytics
- Powerplay and death overs analysis
- Run rate patterns by over
- Boundary and wicket analysis
- Player impact on match outcomes
- Venue scoring characteristics

## 🏗️ Architecture

### Core Components

1. **Scrapers** (`src/cricket_database/scrapers/`)
   - ESPN Cricinfo scraper with Playwright
   - Cricket API scraper with HTTPX
   - Base scraper with rate limiting and retries

2. **ETL Pipeline** (`src/cricket_database/etl/`)
   - Data transformation and validation
   - Database loading with upserts
   - Quality checks and monitoring

3. **Database Models** (`src/cricket_database/models/`)
   - SQLAlchemy ORM models
   - Pydantic validation schemas
   - Comprehensive relationships

4. **CLI Interface** (`src/cricket_database/cli/`)
   - Rich command-line interface
   - Progress tracking and logging
   - Configuration management

5. **SQL Queries** (`sql/queries/`)
   - Organized query library
   - Batting, bowling, team, match analytics
   - Advanced statistical queries

### Data Flow

```
Data Sources → Scrapers → ETL Pipeline → Database → Analytics
     ↓              ↓           ↓            ↓          ↓
ESPN Cricinfo → ESPN Scraper → Transform → MySQL → SQL Queries
Cricket API   → API Scraper  → Validate  → Tables → Statistics
```

## ⚙️ Configuration

### Environment Variables

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=3306
DB_NAME=cricket_db
DB_USER=cricket_user
DB_PASSWORD=your_secure_password

# Scraping Configuration
SCRAPER_RATE_LIMIT=1.0
SCRAPER_RETRY_ATTEMPTS=3
SCRAPER_TIMEOUT=30
SCRAPER_USER_AGENT=CricketDataBot/1.0

# Rate Limiting
MAX_REQUESTS_PER_MINUTE=60
MAX_REQUESTS_PER_HOUR=1000

# Data Quality
ENABLE_DATA_VALIDATION=true
ENABLE_DUPLICATE_CHECK=true
BATCH_SIZE=1000
```

### Make Commands

```bash
# Development
make dev              # Full development setup
make install          # Install dependencies
make dev-install      # Install with dev tools
make setup-db         # Initialize database

# Data Operations
make run-scraper      # Run data scraper
make dry-run          # Test scraper in dry-run mode
make update-incremental  # Incremental update
make update-full      # Full data refresh

# Quality & Testing
make test             # Run tests
make test-cov         # Run tests with coverage
make lint             # Run linting
make format           # Format code
make quality-check    # Run data quality checks

# Maintenance
make clean            # Clean temporary files
make ci               # Run CI pipeline locally
```

## 🧪 Testing

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run specific test categories
pytest tests/unit/           # Unit tests
pytest tests/integration/    # Integration tests
pytest -m "not slow"         # Skip slow tests

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/unit/test_models.py
```

## 📊 Data Quality

The system includes comprehensive data quality checks:

- **Validation**: Pydantic schema validation
- **Deduplication**: Automatic duplicate detection
- **Referential Integrity**: Foreign key validation
- **Data Freshness**: Stale data identification
- **Consistency Checks**: Cross-table validation
- **Quality Scoring**: Overall data quality metrics

## 🔧 Development

### Project Structure

```
cricket-database/
├── src/
│   ├── cricket_database/
│   │   ├── models/          # Database models
│   │   ├── schemas/         # Pydantic schemas
│   │   ├── scrapers/        # Data scrapers
│   │   ├── etl/            # ETL pipeline
│   │   ├── cli/            # CLI interface
│   │   └── utils/          # Utilities
│   └── cli.py              # CLI entry point
├── sql/
│   └── queries/            # SQL query library
├── tests/                  # Test suite
├── logs/                   # Log files
├── data/                   # Data files
├── requirements.txt        # Dependencies
├── pyproject.toml         # Project configuration
├── Makefile               # Build commands
└── README.md              # This file
```

### Adding New Data Sources

1. Create a new scraper class inheriting from `BaseScraper`
2. Implement required methods: `scrape_teams()`, `scrape_players()`, `scrape_matches()`
3. Add to the ETL pipeline configuration
4. Update tests and documentation

### Adding New Analytics

1. Create new query classes in `sql/queries/`
2. Add methods for specific analytics
3. Update the query library exports
4. Add tests and examples

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- ESPN Cricinfo for cricket data
- Playwright for web scraping capabilities
- SQLAlchemy for database ORM
- Pydantic for data validation
- Rich for beautiful CLI interfaces

## 📞 Support

For questions, issues, or contributions:

- Create an issue on GitHub
- Check the documentation
- Review the test suite for examples
- Join the community discussions

---

**Note**: This system is designed for educational and research purposes. Please respect the terms of service of data sources and implement appropriate rate limiting and ethical scraping practices.
