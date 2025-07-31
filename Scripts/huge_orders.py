import pandas as pd
import numpy as np

np.random.seed(42)
num_rows = 5_000_000

df_huge = pd.DataFrame({
    'order_id': np.arange(1, num_rows + 1),
    'customer_id': np.random.randint(1, 100_000, size=num_rows),
    'order_date': pd.to_datetime('2023-01-01') + pd.to_timedelta(np.random.randint(0, 365, size=num_rows), unit='D'),
    'quantity': np.random.randint(1, 100, size=num_rows),
    'price': np.random.uniform(1, 500, size=num_rows),
    'product_category': np.random.choice(['Books', 'Electronics', 'Clothing', 'Toys', 'Groceries'], size=num_rows),
    'country': np.random.choice(['USA', 'UK', 'Canada', 'France', 'Germany'], size=num_rows),
    'payment_method': np.random.choice(['Credit Card', 'PayPal', 'Bank Transfer'], size=num_rows),
    'is_returned': np.random.choice([True, False], size=num_rows),
    'reordered': np.random.choice([True, False], size=num_rows),
})

df_huge['is_returned'] = df_huge['is_returned'].astype(object)
df_huge['reordered'] = df_huge['reordered'].astype(object)

print(df_huge.info(memory_usage="deep"))

# Sauvegarde CSV
df_huge.to_csv("inputs/huge_orders.csv", index=False)
# df_huge.to_parquet("huge_orders.parquet", index=False)

