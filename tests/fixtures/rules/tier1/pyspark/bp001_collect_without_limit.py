"""Test fixture for BP001: collect() without limit() can OOM the driver."""


# This should trigger the rule
def test_trigger():
    df = spark.read.table("some_table")
    result = df.collect()  # Error: collect() without limit()
    return result


# This should NOT trigger the rule
def test_fixed():
    df = spark.read.table("some_table")
    result = df.limit(100).collect()  # Fixed: limit() before collect()
    return result


# Another example that should trigger
def test_trigger_chained():
    data = spark.range(1000)
    collected = data.filter(data.id > 100).collect()  # Error: no limit()


# Fixed version with take() instead
def test_fixed_with_take():
    data = spark.range(1000)
    taken = data.filter(data.id > 100).take(50)  # Fixed: use take()
    return taken
