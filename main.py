# ─── Packages needed ───
import os
import pandas as pd
from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials
from google.api_core.exceptions import AlreadyExists

# ─── Point to the emulator ───
os.environ["SPANNER_EMULATOR_HOST"] = "localhost:9010"

INSTANCE_ID = "test-instance"
DATABASE_ID = "superstore-db"
PROJECT_ID = "test-project"
BATCH_SIZE = 500

# ─── Constructor for the orders data in pandas ───
class OrdersDataFrame:
    def __init__(self):
        self.file_directory = "sample_superstore.xlsx"
        self.orders = pd.read_excel(self.file_directory, sheet_name="Orders")
        self.people = pd.read_excel(self.file_directory, sheet_name="People")
        self.returns = pd.read_excel(self.file_directory, sheet_name="Returns")

    def total_sales(self):
        return self.orders['Sales'].sum()

    def total_quantity(self):
        return self.orders['Quantity'].sum()

# ─── Constructor for the GoogleSpanner database ───
class GoogleSpannerDB:

    def __init__(self, orders_dataframe: OrdersDataFrame):
        self.orders_dataframe = orders_dataframe

        # ─── Connect to the emulator ───
        self.client = spanner.Client(
            project=PROJECT_ID,
            credentials=AnonymousCredentials(),
        )
        self.instance = None
        self.database = None

    # ──────────────────────────────────
    # STEP 1: Create Instance
    # ──────────────────────────────────
    def create_instance(self):
        config_name = f"projects/{PROJECT_ID}/instanceConfigs/emulator-config"
        self.instance = self.client.instance(
            INSTANCE_ID,
            configuration_name=config_name,
            display_name="Test Instance",
        )
        try:
            self.instance.create()
            print(f"✅ Instance '{INSTANCE_ID}' created.")

        except AlreadyExists:
            print(f"ℹ️ Instance '{INSTANCE_ID}' already exists, reusing it.")

    # ──────────────────────────────────
    # STEP 2: Create Database with Schema
    # ──────────────────────────────────

    def create_database(self):
        db = self.instance.database(
            DATABASE_ID,
            ddl_statements=[
                # ─── People table ───
                """CREATE TABLE People
                   (
                       Person STRING(100),
                       Region STRING(50) NOT NULL,
                   ) PRIMARY KEY (Region)""",

                # ─── Orders table (FK → People) ───
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
                       Sales        FLOAT64,
                       Quantity     INT64,
                       Discount     FLOAT64,
                       Profit       FLOAT64,
                       CONSTRAINT FK_Orders_People
                           FOREIGN KEY (Region)
                               REFERENCES People (Region),
                   ) PRIMARY KEY (RowID)""",

                # ─── Returns table (FK → Orders) ───
                """CREATE TABLE Returns
                   (
                       ReturnID INT64 NOT NULL,
                       Returned STRING(10),
                       OrderID  STRING(50),
                   ) PRIMARY KEY (ReturnID)""",
            ],
        )

        try:
            operation = db.create()
            operation.result()
            print(f"✅ Database '{DATABASE_ID}' created.")
        except AlreadyExists:
            print(f"ℹ️ Database '{DATABASE_ID}' already exists, reusing it.")

        self.database = db

    # ──────────────────────────────────
    # STEP 3: Insert Data from DataFrames
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

        # DataFrame column names (must match Excel exactly)
        columns = [
            "RowID", "Order ID", "Order Date", "Ship Date", "Ship Mode",
            "Customer ID", "Customer Name", "Segment", "Country/Region", "City",
            "State/Province", "Postal Code", "Region", "Product ID", "Category",
            "Sub-Category", "Product Name", "Sales", "Quantity",
            "Discount", "Profit",
        ]

        # Spanner column names (must match your CREATE TABLE)
        spanner_columns = [
            "RowID", "OrderID", "OrderDate", "ShipDate", "ShipMode",
            "CustomerID", "CustomerName", "Segment", "Country", "City",
            "State", "PostalCode", "Region", "ProductID", "Category",
            "SubCategory", "ProductName", "Sales", "Quantity",
            "Discount", "Profit",
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
        df["ReturnID"] = range(1, len(df) + 1)

        with self.database.batch() as batch:
            batch.insert_or_update(
                table="Returns",
                columns=["ReturnID", "Returned", "OrderID"],
                values=df[["ReturnID", "Returned", "Order ID"]].values.tolist(),
            )
        print(f"✅ Uploaded {len(df)} rows to 'Returns'.")

if __name__ == '__main__':
    sample_superstore = OrdersDataFrame()
    spanner_database = GoogleSpannerDB(sample_superstore)
    spanner_database.create_instance()
    spanner_database.create_database()
    spanner_database.upload_people()
    spanner_database.upload_orders()
    spanner_database.upload_returns()
