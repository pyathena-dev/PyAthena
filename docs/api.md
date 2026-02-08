(api)=

# API reference

This section provides comprehensive API documentation for all PyAthena classes and functions, organized by functionality.

```{toctree}
:maxdepth: 2
:caption: API Documentation:

api/connection
api/converters
api/utilities
api/errors
api/filesystem
api/models
api/sqlalchemy
api/pandas
api/arrow
api/polars
api/s3fs
api/spark
```

## Quick reference

### Core functionality

- {ref}`api_connection` - Connection management and basic cursors
- {ref}`api_converters` - Data type conversion and parameter formatting
- {ref}`api_utilities` - Utility functions and base classes
- {ref}`api_errors` - Exception handling and error classes

### Infrastructure

- {ref}`api_filesystem` - S3 filesystem integration and object management
- {ref}`api_models` - Athena query execution and metadata models

### Specialized integrations

- {ref}`api_sqlalchemy` - SQLAlchemy dialect implementations
- {ref}`api_pandas` - pandas DataFrame integration
- {ref}`api_arrow` - Apache Arrow columnar data integration
- {ref}`api_polars` - Polars DataFrame integration (no pyarrow required)
- {ref}`api_s3fs` - Lightweight S3FS-based cursor (no pandas/pyarrow required)
- {ref}`api_spark` - Apache Spark integration for big data processing
