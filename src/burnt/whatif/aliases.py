"""Three-letter aliases for what-if builders.

Usage:
    from burnt.whatif.aliases import wrap

    wrapped = wrap(estimate.what_if())

    # Property access (recommended) - returns WhatIfResult directly
    result = wrapped.clsr.enable_photon
    result = wrapped.data.to_delta_format
    result = wrapped.conf.with_aqe_enabled

    # With parameters - use method call
    result = wrapped.clsr.to_instance('Standard_DS4_v2')
    result = wrapped.clsr.use_spot(True)
    result = wrapped.conf.with_shuffle_partitions(200)

    # Full chaining via methods
    result = wrapped.clsr().enable_photon().compare()
"""

from ..estimators.whatif import WhatIfBuilder


class _ClusterAlias:
    """Alias for cluster() method."""

    def __init__(self, builder: WhatIfBuilder):
        self._builder = builder

    def __call__(self):
        """Method call returns ClusterBuilder for full chaining."""
        return self._builder.cluster()

    @property
    def enable_photon(self):
        """Enable Photon - returns WhatIfResult."""
        return self._builder.cluster().enable_photon().compare()

    @property
    def disable_photon(self):
        """Disable Photon - returns WhatIfResult."""
        return self._builder.cluster().disable_photon().compare()

    def to_instance(self, instance_type: str):
        """Change to instance type."""
        return self._builder.cluster().to_instance(instance_type).compare()

    def use_spot(self, fallback: bool = True):
        """Use spot instances."""
        return self._builder.cluster().use_spot(fallback).compare()

    def use_pool(
        self,
        instance_pool_id: str | None = None,
        use_spot: bool = False,
        min_idle: int = 0,
    ):
        """Use instance pool."""
        return (
            self._builder.cluster()
            .use_pool(instance_pool_id, use_spot, min_idle)
            .compare()
        )

    def set_workers(self, count: int):
        """Set worker count."""
        return self._builder.cluster().set_workers(count).compare()

    def to_serverless(self, utilization_pct: float = 50.0):
        """Migrate to serverless."""
        return self._builder.cluster().to_serverless(utilization_pct).compare()


class _DataSourceAlias:
    """Alias for data_source() method."""

    def __init__(self, builder: WhatIfBuilder):
        self._builder = builder

    def __call__(self):
        """Method call returns DataSourceBuilder for full chaining."""
        return self._builder.data_source()

    @property
    def to_delta_format(self):
        """Migrate to Delta format."""
        return self._builder.data_source().to_delta_format().compare()

    def enable_liquid_clustering(self, keys: list[str]):
        """Enable Liquid Clustering."""
        return self._builder.data_source().enable_liquid_clustering(keys).compare()

    def set_partitioning(self, column: str):
        """Set partitioning."""
        return self._builder.data_source().set_partitioning(column).compare()

    @property
    def enable_disk_cache(self):
        """Enable disk cache."""
        return self._builder.data_source().enable_disk_cache().compare()

    def compact_files(self, target_mb: int = 128):
        """Compact files."""
        return self._builder.data_source().compact_files(target_mb).compare()

    @property
    def enable_column_pruning(self):
        """Enable column pruning."""
        return self._builder.data_source().enable_column_pruning().compare()

    @property
    def enable_file_skipping(self):
        """Enable file skipping."""
        return self._builder.data_source().enable_file_skipping().compare()

    def set_compression(self, codec: str = "zstd"):
        """Set compression."""
        return self._builder.data_source().set_compression(codec).compare()


class _SparkConfigAlias:
    """Alias for spark_config() method."""

    def __init__(self, builder: WhatIfBuilder):
        self._builder = builder

    def __call__(self):
        """Method call returns SparkConfigBuilder for full chaining."""
        return self._builder.spark_config()

    def with_shuffle_partitions(self, count: int):
        """Set shuffle partitions."""
        return self._builder.spark_config().with_shuffle_partitions(count).compare()

    @property
    def with_auto_shuffle_partitions(self):
        """Enable auto shuffle partitions."""
        return self._builder.spark_config().with_auto_shuffle_partitions().compare()

    def with_broadcast_threshold_mb(self, mb: int):
        """Set broadcast threshold."""
        return self._builder.spark_config().with_broadcast_threshold_mb(mb).compare()

    @property
    def with_aqe_enabled(self):
        """Enable AQE."""
        return self._builder.spark_config().with_aqe_enabled().compare()

    def set(self, key: str, value: str | int | bool):
        """Set arbitrary config."""
        return self._builder.spark_config().set(key, value).compare()


class _WhatIfBuilderAlias:
    """Wrapper that provides 3-letter aliases."""

    def __init__(self, builder: WhatIfBuilder):
        self._builder = builder

    @property
    def clsr(self) -> _ClusterAlias:
        """Alias for cluster()."""
        return _ClusterAlias(self._builder)

    @property
    def data(self) -> _DataSourceAlias:
        """Alias for data_source()."""
        return _DataSourceAlias(self._builder)

    @property
    def conf(self) -> _SparkConfigAlias:
        """Alias for spark_config()."""
        return _SparkConfigAlias(self._builder)

    def cluster(self):
        return self._builder.cluster()

    def data_source(self):
        return self._builder.data_source()

    def spark_config(self):
        return self._builder.spark_config()

    def scenarios(self, scenarios):
        return self._builder.scenarios(scenarios)

    def options(self):
        return self._builder.options()

    def compare(self):
        return self._builder.compare()


def wrap(builder: WhatIfBuilder) -> _WhatIfBuilderAlias:
    """Wrap a WhatIfBuilder to provide 3-letter aliases."""
    return _WhatIfBuilderAlias(builder)
