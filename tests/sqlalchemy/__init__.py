from sqlalchemy.dialects import registry

registry.register("awsathena", "pyathena.sqlalchemy.base", "AthenaDialect")
registry.register("awsathena.rest", "pyathena.sqlalchemy.rest", "AthenaRestDialect")
registry.register("awsathena.pandas", "pyathena.sqlalchemy.pandas", "AthenaPandasDialect")
registry.register("awsathena.arrow", "pyathena.sqlalchemy.arrow", "AthenaArrowDialect")
registry.register("awsathena.s3fs", "pyathena.sqlalchemy.s3fs", "AthenaS3FSDialect")
registry.register("awsathena.aiorest", "pyathena.aio.sqlalchemy.rest", "AthenaAioRestDialect")
registry.register("awsathena.aiopandas", "pyathena.aio.sqlalchemy.pandas", "AthenaAioPandasDialect")
registry.register("awsathena.aioarrow", "pyathena.aio.sqlalchemy.arrow", "AthenaAioArrowDialect")
registry.register("awsathena.aiopolars", "pyathena.aio.sqlalchemy.polars", "AthenaAioPolarsDialect")
registry.register("awsathena.aios3fs", "pyathena.aio.sqlalchemy.s3fs", "AthenaAioS3FSDialect")
