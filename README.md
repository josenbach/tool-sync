# Ion Tool Synchronization System

This system provides comprehensive tool synchronization between TipQA and Ion environments. It consists of several scripts designed for different purposes:

- **Daily Tool Sync**: Handles ongoing synchronization between TipQA and Ion V1 environments
- **V2 Population**: One-time population script for V2 Sandbox and V2 Production environments
- **Ion Tool Library Builder**: Creates tool parts (without inventory) in V2 Sandbox and V2 Production (Ion Reloaded)

## Supported Environments

The system supports the following Ion environments:

1. **V1 Sandbox**: Testing environment for V1 Ion API (`staging-api.buildwithion.com`)
   - Credentials: `V1SANDBOX_CLIENT` / `V1SANDBOX_SECRET`
   
2. **V1 Production** (One Ion): Production environment for V1 Ion API (`api.buildwithion.com`)
   - Credentials: `V1CLIENT` / `V1SECRET`
   - Used for: Daily tool synchronization in production
   
3. **V2 Sandbox**: Testing environment for V2 Ion API (`staging-api.buildwithion.com`)
   - Credentials: `V2SANDBOX_CLIENT` / `V2SANDBOX_SECRET`
   
4. **V2 Production** (Ion Reloaded): Production environment for V2 Ion API (`api-jobyaero.buildwithion.com`)
   - Credentials: `V2CLIENT` / `V2SECRET`
   - Used for: Joby Aviation's custom Ion instance
   
5. **V2 Staging** (Ion Reloaded Staging): Staging environment for V2 Ion API (`staging-api.buildwithion.com`)
   - Credentials: `V2STAGINGCLIENT` / `V2STAGINGSECRET`

## Key Features

- **Tool Synchronization**: Syncs tools between TipQA and Ion systems
- **Tool Creation**: Creates new tools in Ion that don't exist
- **Tool Updates**: Updates existing tools based on TipQA data
- **Orphaned Tool Management**: Identifies and marks tools as unavailable that exist in Ion but not in TipQA
- **Status Management**: Handles tool statuses (available/unavailable) based on TipQA maintenance status
- **Comprehensive Error Handling**: Detailed error reporting and diagnostic output
- **Diagnostic Reports**: Markdown reports with field-level analysis and service interval changes

## Prerequisites

- Python 3.8+
- Access to TipQA SQL Server
- Ion API credentials (sandbox and production)
- Docker (for containerized deployment)

## Setup

### 1. Environment Configuration

Copy the environment template and fill in your credentials:

```bash
cp env_template.txt .env
```

Edit `.env` with your actual credentials. See `env_template.txt` for all required environment variables and their descriptions.

### 2. Configuration

Edit `config.yaml` to match your environment. The file includes:
- Database connection settings for TipQA
- Ion API endpoints for all environments
- Protected part numbers configuration
- Other system settings

See `config.yaml` in the repository for the complete configuration template.

## Scripts

### 1. Daily Tool Sync (`daily_tool_sync.py`)

**Purpose**: Ongoing synchronization between TipQA and Ion V1 environments
**Usage**: Run daily for production tool management

**Supported Environments**:
- **V1 Sandbox**: Use for testing changes before production
- **V1 Production** (One Ion): Use for daily production synchronization

**Features**:
- Syncs tools from TipQA to Ion V1 Sandbox or V1 Production
- Handles orphaned tools (tools in Ion but not in TipQA)
- Comprehensive error handling and logging
- Diagnostic reports for analysis

**Documentation**:
- **Master Data Flow**: `queries/master_data_flow.md` - Complete documentation of master dataframe creation and analysis logic
- **Execution Flow**: `tests/test_logic_flow.md` - Complete documentation of how scripts execute updates

**Usage**:
```bash
# Specify environment when running (v1_sandbox or v1_production)
# Check script for environment parameter usage
```

### 2. V2 Population (`populate_v2_from_tipqa.py`)

**Purpose**: One-time population of V2 Sandbox and V2 Production environments
**Usage**: Run once to initially populate V2 environments, then archive

**Features**:
- Populates V2 Sandbox from TipQA
- Populates V2 Production from TipQA
- Skips obsolete format tools (XXXX-XXXX)
- Comprehensive validation and error handling

### 3. Ion Tool Library Builder (`utilities/ion_tool_library_builder.py`)

**Purpose**: Creates tool parts (without inventory) in V2 Sandbox or V2 Production (Ion Reloaded)
**Usage**: Run to build the tool library before creating inventory items

**Features**:
- Creates tool parts without serial numbers (library parts only)
- Supports both V2 Sandbox and V2 Production (Ion Reloaded) environments
- Excludes inactive tools (maintenance_status = 'I' or revision_status = 'I')
- Filters out protected part numbers and their associated serial numbers
- Maps TipQA fields: partNumber, revision, description, maintenanceIntervalSeconds, and Asset Type attribute
- Multiple safety checks to prevent protected parts from being created
- Dry run mode for analysis

**Usage**:
```bash
# Dry run for V2 Sandbox (default)
python utilities/ion_tool_library_builder.py --dry-run

# Dry run for V2 Production (Ion Reloaded)
python utilities/ion_tool_library_builder.py --environment v2_production --dry-run

# Actual execution for V2 Production
python utilities/ion_tool_library_builder.py --environment v2_production

# Generate invalid revision report
python utilities/ion_tool_library_builder.py --invalid-revision-report
```

**Field Mappings**: See `queries/master_data_flow.md` for complete field mapping documentation.

## Usage

### Quick Start

1. **Install dependencies**:
   ```bash
   make install
   ```

2. **Set up environment**:
   ```bash
   make setup
   # Edit .env with your credentials
   ```

3. **Run daily sync (dry run)**:
   ```bash
   make daily-sync
   ```

4. **Run V2 population (dry run)**:
   ```bash
   make populate-v2
   ```

### Production Usage

1. **Daily sync in production**:
   ```bash
   make daily-sync-prod
   ```

2. **V2 population in production**:
   ```bash
   make populate-v2-prod
   ```

### Docker Deployment

1. **Build and run**:
   ```bash
   make run
   ```

2. **Clean up**:
   ```bash
   make clean
   ```

### Cloud Deployment

This script is designed to run daily from a cloud server. You can:

1. **Set up a cron job**:
   ```bash
   # Run daily at 2 AM
   0 2 * * * cd /path/to/ion_tool_population && make run
   ```

2. **Use cloud schedulers** like AWS EventBridge, Azure Logic Apps, or Google Cloud Scheduler

3. **Deploy as a container** using Docker

## How It Works

The system follows a structured process:

1. **Master Dataframe Creation** - Combines TipQA and Ion data into unified dataset
2. **Analysis Logic** - Determines actions (SKIP, CREATE, UPDATE, etc.) for each tool
3. **Execution** - Performs Ion mutations based on determined actions

For complete details, see:
- `queries/master_data_flow.md` - Master dataframe creation and analysis logic
- `tests/test_logic_flow.md` - Execution details and testing methodology

## Logging and Reports

The scripts generate:
- **Terminal Output**: Real-time progress and summary statistics
- **Diagnostic Reports**: Markdown files in `tests/` directory with detailed analysis
- **CSV Reports**: For dry-run analysis (when applicable)
- **Error Reports**: Detailed error information for failed operations

### Diagnostic Reports

After each run, check `tests/update_diagnostic_YYYYMMDD_HHMMSS.md` for:
- Summary statistics
- Update reasons breakdown
- Field-level mismatch analysis
- Service interval changes (for calibration team review)
- Sample comparisons

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check SQL Server credentials in `.env`
   - Verify network connectivity to TipQA server

2. **Ion API Authentication Failed**
   - Verify API credentials in `.env`
   - Check if credentials are for correct environment

3. **GraphQL Errors**
   - Check diagnostic reports in `tests/` directory
   - Review error details in `update_diagnostic_YYYYMMDD_HHMMSS.md`
   - Verify GraphQL queries in `queries/` folder

4. **High Update Counts**
   - Review diagnostic report for field-level mismatch breakdown
   - Check service interval changes section
   - Verify comparison logic in `queries/master_data_flow.md`

## File Structure

```
ion_tool_population/
├── daily_tool_sync.py          # Main daily synchronization script
├── config.yaml                  # Configuration file
├── requirements.txt             # Python dependencies
├── Dockerfile                   # Container configuration
├── docker-compose.yml          # Docker compose setup
├── Makefile                     # Build commands
├── env_template.txt            # Environment variables template
├── utilities/                   # Utility scripts
│   ├── database_utils.py        # Database connection utilities
│   ├── graphql_utils.py         # GraphQL query utilities
│   ├── shared_sync_utils.py     # Core synchronization logic
│   └── tool_processing_utils.py # Tool creation/update functions
├── tests/                       # Test scripts and diagnostic reports
│   └── test_logic_flow.md       # Execution flow documentation
├── queries/                     # All queries (GraphQL and SQL)
│   ├── *.graphql                # GraphQL queries
│   ├── *.sql                    # SQL queries
│   └── master_data_flow.md      # Master dataframe creation documentation
└── README.md                    # This file (setup and usage)
```

**Key Documentation**:
- **Setup/Usage**: `README.md` (this file)
- **Master Dataframe**: `queries/master_data_flow.md` - How master dataframe is created
- **Execution**: `tests/test_logic_flow.md` - How scripts execute updates

## Documentation

For detailed information:

- **Master Dataframe Creation**: `queries/master_data_flow.md` - How TipQA and Ion data are combined and analyzed
- **Execution Details**: `tests/test_logic_flow.md` - How scripts perform updates, mutations, and error handling
- **Tool Sync Logic**: `utilities/tool_sync_logic_flow.md` - Additional synchronization details

## Security Considerations

- Store credentials securely (use environment variables or secret management)
- Use sandbox environment for testing
- Monitor logs for unauthorized access attempts
- Regularly rotate API credentials
- Use least-privilege access for database connections

## Support

For issues or questions:

1. Check the logs in `ion_tool_population.log`
2. Verify configuration files
3. Test in sandbox environment first
4. Review Ion API documentation for any changes

## Protected Parts

Certain part numbers are protected from being created or converted to tools. These are defined in `config.yaml` under `sync_exceptions.protected_part_numbers`. See `queries/master_data_flow.md` for details on how protected parts are handled.

## Version History

- **v2.0** (2025-01-XX): Added V2 Production (Ion Reloaded) support, Ion Tool Library Builder, protected parts filtering
- **v1.0** (2025-01-27): Initial release with full population capabilities
