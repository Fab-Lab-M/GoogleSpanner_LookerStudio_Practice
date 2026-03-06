# ─── Packages needed ───
import os
import pandas as pd
from decimal import Decimal
from google.cloud import spanner
from google.api_core.exceptions import AlreadyExists
from dotenv import load_dotenv

# ─── Load Environment Variables ───
load_dotenv()

# ─── Point to the instance ───
PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
INSTANCE_ID = os.getenv("SPANNER_INSTANCE_ID")
DATABASE_ID = os.getenv("SPANNER_DATABASE_ID", "superstore-db")
BATCH_SIZE = 500

# ─── Orders data in pandas ───
class OrdersDataFrame:
    def __init__(self):
        self.file_directory = "sample_superstore.xlsx"
        print("📂 Loading Excel file...")
        self.orders = pd.read_excel(self.file_directory, sheet_name="Orders")
        self.people = pd.read_excel(self.file_directory, sheet_name="People")
        self.returns = pd.read_excel(self.file_directory, sheet_name="Returns")

        # Filter out Canada rows. We only want to analyze the US sales.
        self.orders = self.orders[self.orders["Country/Region"] != "Canada"]

        # Filter out the data from after 2025-10-31. Mainly to test n scenario in the visualization
        self.orders = self.orders[self.orders["Order Date"] <= "2025-10-31"]

        # Added to make this columns NUMERIC in the DDL
        cols_to_fix = ['Sales', 'Profit', 'Discount']
        for col in cols_to_fix:
            self.orders[col] = self.orders[col].apply(lambda x: Decimal(str(round(x, 5))))

    # ─── Methods to make the cross-check process───
    def total_sales(self):
        return self.orders['Sales'].sum()

    def total_quantity(self):
        return self.orders['Quantity'].sum()

    def sales_by_year(self):
        return self.orders.groupby(self.orders['Order Date'].dt.year)['Sales'].sum()

    def sales_by_category(self):
        return self.orders.groupby('Category')['Sales'].sum()

    def sales_by_subcategory(self):
        return self.orders.groupby(['Category','Sub-Category'])['Sales'].sum()

# ─── Constructor for the GoogleSpanner database ───
class GoogleSpannerDB:

    def __init__(self, orders_dataframe: OrdersDataFrame):
        self.orders_dataframe = orders_dataframe

        self.client = spanner.Client(project=PROJECT_ID)
        self.instance = self.client.instance(INSTANCE_ID)
        self.database = None

    # ──────────────────────────────────
    # STEP 1: Create Database with Schema
    # ──────────────────────────────────

    def create_database(self):
        database_ddl = [
            # ─── People table ───
            """CREATE TABLE People
               (
                   Person STRING(100),
                   Region STRING(50),
               ) PRIMARY KEY (Region)""",

            # ─── Orders table ───
            """CREATE TABLE Orders
               (
                   RowID        INT64 NOT NULL,
                   OrderID      STRING(50),
                   OrderDate    DATE,
                   ShipDate     DATE,
                   ShipMode     STRING(50),
                   CustomerID   STRING(50),
                   CustomerName STRING(100),
                   Segment      STRING(50),
                   Country      STRING(50),
                   City         STRING(100),
                   State        STRING(50),
                   PostalCode   STRING(20),
                   Region       STRING(50),
                   ProductID    STRING(50),
                   Category     STRING(50),
                   SubCategory  STRING(50),
                   ProductName  STRING(MAX),
                   Sales        NUMERIC,
                   Quantity     INT64,
                   Discount     NUMERIC,
                   Profit       NUMERIC,
                   CONSTRAINT FK_Orders_Region FOREIGN KEY (Region) REFERENCES People (Region)
               ) PRIMARY KEY (OrderID, RowID)""",

            # ─── Returns table (standalone, no interleave) ───
            """CREATE TABLE Returns
               (
                   OrderID  STRING(50) NOT NULL,
                   Returned STRING(10),
               ) PRIMARY KEY (OrderID)"""
        ]

        try:
            db = self.instance.database(
                DATABASE_ID,
                ddl_statements=database_ddl,
            )
            db.create().result()
            print(f"✅ Database '{DATABASE_ID}' created.")
        except AlreadyExists:
            db = self.instance.database(DATABASE_ID)
            print(f"ℹ️ Database '{DATABASE_ID}' already exists, reusing it.")

        self.database = db

    # ──────────────────────────────────
    # STEP 2: Insert Data from DataFrames
    # ──────────────────────────────────
    def upload_people(self):
        df = self.orders_dataframe.people
        with self.database.batch() as batch:
            batch.insert_or_update(
                table="People",
                columns=["Person", "Region"],
                values=df[["Regional Manager", "Region"]].values.tolist(),
            )
        print(f"✅ Uploaded {len(df)} rows to 'People'.")

    def upload_orders(self):
        df = self.orders_dataframe.orders.copy()
        df["RowID"] = range(1, len(df) + 1)
        df["Order Date"] = df["Order Date"].dt.strftime("%Y-%m-%d")
        df["Ship Date"] = df["Ship Date"].dt.strftime("%Y-%m-%d")
        df["Postal Code"] = df["Postal Code"].fillna("").astype(str)

        columns = [
            "RowID", "Order ID", "Order Date", "Ship Date", "Ship Mode",
            "Customer ID", "Customer Name", "Segment", "Country/Region", "City",
            "State/Province", "Postal Code", "Region", "Product ID", "Category",
            "Sub-Category", "Product Name", "Sales", "Quantity",
            "Discount", "Profit"
        ]

        spanner_columns = [
            "RowID", "OrderID", "OrderDate", "ShipDate", "ShipMode",
            "CustomerID", "CustomerName", "Segment", "Country", "City",
            "State", "PostalCode", "Region", "ProductID", "Category",
            "SubCategory", "ProductName", "Sales", "Quantity",
            "Discount", "Profit"
        ]

        for i in range(0, len(df), BATCH_SIZE):
            chunk = df.iloc[i:i + BATCH_SIZE]
            with self.database.batch() as batch:
                batch.insert_or_update(
                    table="Orders",
                    columns=spanner_columns,
                    values=chunk[columns].values.tolist(),
                )
        print(f"✅ Uploaded {len(df)} rows to 'Orders'.")

    def upload_returns(self):
        df = self.orders_dataframe.returns.copy()

        with self.database.batch() as batch:
            batch.insert_or_update(
                table="Returns",
                columns=["OrderID", "Returned"],
                values=df[["Order ID", "Returned"]].values.tolist(),
            )
        print(f"✅ Uploaded {len(df)} rows to 'Returns'.")

if __name__ == '__main__':
    sample_superstore = OrdersDataFrame()
    print(sample_superstore.sales_by_year())
    print(sample_superstore.sales_by_category())
    print(sample_superstore.sales_by_subcategory())
    spanner_database = GoogleSpannerDB(sample_superstore)
    spanner_database.create_database()
    spanner_database.upload_people()
    spanner_database.upload_orders()
    spanner_database.upload_returns()