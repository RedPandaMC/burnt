# Research: Pythonic Fluent Builder Pattern Analysis

## 1. Is the pattern `.config_type.x().y().z()` Pythonic?

**Yes, the fluent builder pattern is Pythonic when implemented correctly.**

### Evidence from Popular Python Libraries

#### SQLAlchemy (Query Builder)
```python
# SQLAlchemy ORM uses fluent chaining extensively
query = (
    session.query(User)
    .join(Address)
    .filter(User.name == "John")
    .order_by(User.created_at.desc())
    .limit(10)
)
```
This is widely considered Pythonic because:
- **Readability**: Reads like a sentence
- **Discoverability**: Methods are chained logically
- **Immutability**: Each method returns a new query object

#### Pandas/Polars (Data Processing)
```python
# Pandas
df = (pd.read_csv("data.csv")
      .query("age > 18")
      .groupby("department")
      .agg({"salary": "mean"})
      .reset_index())

# Polars
df = (pl.scan_csv("data.csv")
      .filter(pl.col("age") > 18)
      .group_by("department")
      .agg(pl.col("salary").mean())
      .collect())
```

#### AWS SDK (Boto3 Resource Interface)
```python
# Boto3 resource interface (higher-level abstraction)
bucket = s3.Resource('bucket')
obj = bucket.Object('key')
obj.put(Body=b'body')
```

### Pythonic Principles Alignment

| Principle | Fluent Builder Alignment |
|-----------|-------------------------|
| **"Flat is better than nested"** | ✅ Chain depth linear vs nested config objects |
| **"Explicit is better than implicit"** | ✅ Method names are self-documenting |
| **"Readability counts"** | ✅ Natural language flow |
| **"Namespaces are great"** | ✅ Method names form clear namespace |

## 2. Alternative Patterns for Category Separation

### Pattern A: Direct Configuration (Simple)
```python
# Configuration via parameters
estimate = burnt.estimate(
    query="SELECT ...",
    enable_photon=True,
    instance_type="Standard_DS3_v2",
    use_spot=True
)
```
**Pros**: Simple, explicit
**Cons**: Becomes unwieldy with many options, hard to validate order

### Pattern B: Dictionary/JSON Configuration
```python
# Configuration via dict
config = {
    "enable_photon": True,
    "instance_type": "Standard_DS3_v2",
    "use_spot": True
}
estimate = burnt.what_if(config)
```
**Pros**: Serializable, good for external configs
**Cons**: No type safety, keys not IDE-friendly

### Pattern C: Fluent Builder (Recommended for burnt)
```python
# Fluent method chaining
result = (
    estimate.what_if()
    .enable_photon()
    .downsize_to("Standard_DS3_v2", num_workers=3)
    .use_spot(fallback=True)
    .compare()
)
```
**Pros**: Readable, discoverable, supports validation between steps
**Cons**: More methods to implement

### Pattern D: Context Manager
```python
# Context manager pattern
with estimate.what_if() as builder:
    builder.enable_photon()
    builder.downsize_to("Standard_DS3_v2")
result = builder.compare()
```
**Pros**: Clear scope, supports complex setup
**Cons**: More verbose, less fluid

## 3. Property-Based vs Method-Based Approaches

### Property-Based (Attribute Access)
```python
class WhatIfBuilder:
    def __init__(self, estimate):
        self._estimate = estimate
        self._photon_enabled = False

    @property
    def photon(self):
        """Enable photon optimization."""
        self._photon_enabled = True
        return self

    @property
    def spot(self):
        """Use spot instances."""
        self._use_spot = True
        return self

# Usage
result = estimate.what_if().photon.spot.compare()
```
**Pros**: Very concise, reads naturally
**Cons**: 
- Properties should be nouns, not verbs (semantic mismatch)
- Properties shouldn't have side effects (violates principle)
- IDE autocomplete less helpful for actions

### Method-Based (Recommended)
```python
class WhatIfBuilder:
    def __init__(self, estimate):
        self._estimate = estimate
        self._config = {}

    def enable_photon(self) -> "WhatIfBuilder":
        """Enable Photon optimization."""
        self._config["photon"] = True
        return self

    def use_spot(self, fallback: bool = True) -> "WhatIfBuilder":
        """Use spot instances with on-demand fallback."""
        self._config["spot"] = {"fallback": fallback}
        return self

# Usage
result = (
    estimate.what_if()
    .enable_photon()
    .use_spot(fallback=True)
    .compare()
)
```
**Pros**:
- Semantic correctness (methods are verbs)
- Type hints work well
- Can accept parameters
- Clear intent

## 4. Pythonic Principles Deep Dive

### "Flat is better than nested"
```python
# ❌ Nested (less Pythonic)
result = estimate.what_if(
    photon=estimate.what_if().enable_photon(),
    cluster=estimate.what_if().downsize_to("DS3_v2")
)

# ✅ Flat (more Pythonic)
result = (
    estimate.what_if()
    .enable_photon()
    .downsize_to("Standard_DS3_v2")
    .compare()
)
```

### Readability and Intent
```python
# ❌ Obscure configuration
result = estimate.what_if({"photon": True, "downsize": "DS3_v2"})

# ✅ Self-documenting
result = (
    estimate.what_if()
    .enable_photon()
    .downsize_to("Standard_DS3_v2")
    .compare()
)
```

## 5. Recommendations for `burnt`

Based on the research and `burnt`'s current architecture (`src/burnt/estimators/whatif.py`), here's the recommended pattern:

### Recommended Implementation

```python
# src/burnt/estimators/whatif.py

class WhatIfBuilder:
    """Fluent builder for what-if scenarios."""
    
    def __init__(self, estimate: CostEstimate):
        self._estimate = estimate
        self._scenarios: list[CostEstimate] = []
    
    def enable_photon(self, query_type: str = "complex_join") -> "WhatIfBuilder":
        """Enable Photon optimization for the query."""
        photon_estimate = apply_photon_scenario(self._estimate, query_type)
        self._scenarios.append(photon_estimate)
        return self
    
    def downsize_to(
        self, 
        instance_type: str, 
        num_workers: int | None = None
    ) -> "WhatIfBuilder":
        """Downsize cluster to specified instance type."""
        # Implementation here
        return self
    
    def use_spot(self, fallback: bool = True) -> "WhatIfBuilder":
        """Use spot instances with on-demand fallback."""
        # Implementation here
        return self
    
    def compare(self) -> ComparisonResult:
        """Compare all configured scenarios."""
        return ComparisonResult(
            base=self._estimate,
            scenarios=self._scenarios
        )
```

### Usage in `burnt`
```python
# DESIGN.md example (Sprint 2)
result = (
    estimate.what_if()
    .enable_photon()
    .downsize_to("Standard_DS3_v2", num_workers=3)
    .use_spot(fallback=True)
    .compare()
)
```

### Why This Pattern for `burnt`
1. **Matches existing design**: `DESIGN.md` explicitly calls for "Fluent chaining: `.enable_photon().downsize_to().use_spot().compare()`"
2. **Readability**: Data engineers can read the configuration flow naturally
3. **Discoverability**: IDE autocomplete shows available options
4. **Validation**: Each step can validate configuration before proceeding
5. **Flexibility**: Easy to add new scenarios without breaking existing code

## 6. When NOT to Use Fluent Builders

Fluent builders are NOT appropriate when:
1. **Simple configuration**: 1-2 parameters only
2. **Performance critical**: Each method call adds overhead
3. **Validation requires full context**: Some validations need all parameters at once
4. **Immutable final object**: If you need to return a different type at each step

## Conclusion

The `.config_type.x().y().z()` pattern is **Pythonic** and appropriate for `burnt`'s `WhatIfBuilder` because:

1. ✅ Follows established patterns in Python ecosystem (SQLAlchemy, Pandas, Polars)
2. ✅ Aligns with Python's Zen principles
3. ✅ Provides excellent readability for configuration flows
4. ✅ Supports discoverability and IDE support
5. ✅ Matches the explicit design goals in `DESIGN.md`

**Recommendation**: Implement the fluent builder pattern as specified in Sprint 2 of `DESIGN.md`, using method-based chaining rather than property-based for semantic correctness and parameter support.
