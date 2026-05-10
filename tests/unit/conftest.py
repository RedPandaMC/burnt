import pytest


@pytest.fixture
def sample_sql_queries():
    return {
        "simple_select": "SELECT * FROM users WHERE id = 1",
        "cross_join": "SELECT * FROM a CROSS JOIN b",
        "merge_into": "MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name",
        "window_function": "SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) FROM employees",
        "group_by": "SELECT dept, COUNT(*) FROM employees GROUP BY dept",
        "order_by": "SELECT * FROM users ORDER BY created_at",
        "distinct": "SELECT DISTINCT category FROM products",
        "cte": "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
        "subquery": "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
    }
