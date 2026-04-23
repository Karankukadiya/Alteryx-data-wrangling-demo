import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

def add_comment(step_number: int, note: str) -> None:
    print(f"[Step {step_number}] {note}")

# ------------------------------------------------------------
# Step 1: Read input datasets
# ------------------------------------------------------------
add_comment(1, "Reading sales, customer, and product datasets")
sales = pd.read_csv(DATA_DIR / "sales.csv")
customers = pd.read_csv(DATA_DIR / "customer.csv")
products = pd.read_csv(DATA_DIR / "product.csv")

# ------------------------------------------------------------
# Step 2: Standardize column names and trim whitespace
# ------------------------------------------------------------
add_comment(2, "Standardizing text columns and trimming spaces")
for df in [sales, customers, products]:
    df.columns = [col.strip().lower() for col in df.columns]

text_cols = {
    "sales": ["customer_id", "product_id", "region"],
    "customers": ["customer_id", "customer_name", "gender", "city", "state", "segment"],
    "products": ["product_id", "product_name", "category", "sub_category"],
}

for col in text_cols["sales"]:
    sales[col] = sales[col].astype(str).str.strip()
for col in text_cols["customers"]:
    customers[col] = customers[col].astype(str).str.strip()
for col in text_cols["products"]:
    products[col] = products[col].astype(str).str.strip()

# Replace placeholder strings with missing values
sales = sales.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
customers = customers.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
products = products.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

# ------------------------------------------------------------
# Step 3: Data cleansing
# ------------------------------------------------------------
add_comment(3, "Cleaning missing values and fixing data formats")
sales["order_date"] = pd.to_datetime(sales["order_date"], errors="coerce")
customers["city"] = customers["city"].fillna("Unknown")
sales["quantity"] = pd.to_numeric(sales["quantity"], errors="coerce")
sales["sales_amount"] = pd.to_numeric(sales["sales_amount"], errors="coerce")
products["unit_price"] = pd.to_numeric(products["unit_price"], errors="coerce")

# ------------------------------------------------------------
# Step 4: Remove duplicates
# ------------------------------------------------------------
add_comment(4, "Removing duplicate customer and product rows")
customers = customers.drop_duplicates(subset=["customer_id"])
products = products.drop_duplicates(subset=["product_id"])

# ------------------------------------------------------------
# Step 5: Create calculated fields
# ------------------------------------------------------------
add_comment(5, "Creating calculated fields for quality checks and profitability")
sales["calc_revenue"] = sales["quantity"] * sales["sales_amount"]
sales["is_valid_sale"] = (
    sales["customer_id"].notna() &
    sales["product_id"].notna() &
    sales["quantity"].notna() &
    (sales["quantity"] > 0) &
    sales["order_date"].notna()
)

# ------------------------------------------------------------
# Step 6: Filter invalid rows
# ------------------------------------------------------------
add_comment(6, "Filtering out incomplete or invalid sales records")
valid_sales = sales[sales["is_valid_sale"]].copy()

# ------------------------------------------------------------
# Step 7: Join sales with customer
# ------------------------------------------------------------
add_comment(7, "Joining sales with customer dataset on customer_id")
sales_customer = valid_sales.merge(customers, on="customer_id", how="left", indicator=False)

# ------------------------------------------------------------
# Step 8: Join previous result with product
# ------------------------------------------------------------
add_comment(8, "Joining previous result with product dataset on product_id")
final_df = sales_customer.merge(products, on="product_id", how="left", indicator=False)

# ------------------------------------------------------------
# Step 9: Validate and output
# ------------------------------------------------------------
add_comment(9, "Running output validation and exporting final dataset")
final_df["join_status"] = final_df.apply(
    lambda row: "Matched"
    if pd.notna(row["customer_name"]) and pd.notna(row["product_name"])
    else "Unmatched",
    axis=1
)

summary = {
    "raw_sales_rows": int(len(sales)),
    "valid_sales_rows": int(len(valid_sales)),
    "customer_rows_after_dedup": int(len(customers)),
    "product_rows_after_dedup": int(len(products)),
    "final_rows": int(len(final_df)),
    "matched_rows": int((final_df["join_status"] == "Matched").sum()),
    "invalid_rows_removed": int(len(sales) - len(valid_sales)),
}

final_df.to_csv(OUTPUT_DIR / "final_wrangled_dataset.csv", index=False)
pd.DataFrame([summary]).to_csv(OUTPUT_DIR / "workflow_summary.csv", index=False)

# ------------------------------------------------------------
# Step 10: Inline workflow notes
# ------------------------------------------------------------
add_comment(10, "Workflow complete; summary and final dataset saved to output folder")
print("\nWorkflow summary:")
for key, value in summary.items():
    print(f"- {key}: {value}")
