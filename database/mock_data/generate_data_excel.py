# database/mock_data/generate_data_excel.py

import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


# Make results reproducible for your portfolio
random.seed(42)

# Output folder: database/mock_data/csv/
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "csv"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------------------
# Helper: save DataFrame as CSV with a small log message
# -------------------------------------------------------------------
def save_csv(df: pd.DataFrame, name: str) -> None:
    path = OUTPUT_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"Saved {name}.csv with {len(df):,} rows -> {path}")


# -------------------------------------------------------------------
# DimDate
# -------------------------------------------------------------------
def make_dim_date(start: date = date(2024, 1, 1),
                  end: date = date(2025, 12, 31)):
    rows = []
    date_rows = []  # list of (datekey, fulldate)

    d = start
    while d <= end:
        date_key = int(d.strftime("%Y%m%d"))
        year = d.year
        quarter = (d.month - 1) // 3 + 1
        month = d.month
        month_name = d.strftime("%B")
        day_of_month = d.day
        day_of_week = d.isoweekday()  # 1-7
        day_name = d.strftime("%A")
        is_weekday = day_of_week <= 5

        rows.append(
            dict(
                datekey=date_key,
                fulldate=d,
                year=year,
                quarter=quarter,
                month=month,
                monthname=month_name,
                dayofmonth=day_of_month,
                dayofweek=day_of_week,
                dayname=day_name,
                isweekday=is_weekday,
            )
        )
        date_rows.append((date_key, d))
        d += timedelta(days=1)

    df = pd.DataFrame(rows)
    date_keys = [r[0] for r in date_rows]
    month_first_keys = sorted(
        {int(r[1].replace(day=1).strftime("%Y%m%d")) for r in date_rows}
    )
    return df, date_rows, date_keys, month_first_keys


# -------------------------------------------------------------------
# Simple dimensions with surrogate keys
# -------------------------------------------------------------------
def make_dim_region():
    regions = [
        ("FR", "France", "Europe", "Nice"),
        ("DE", "Germany", "Europe", "Munich"),
        ("SG", "Singapore", "APAC", "Singapore"),
        ("US", "United States", "North America", "New York"),
    ]
    rows = []
    for i, (cc, cn, rn, city) in enumerate(regions, start=1):
        rows.append(
            dict(
                regionkey=i,
                countrycode=cc,
                countryname=cn,
                regionname=rn,
                cityname=city,
            )
        )
    df = pd.DataFrame(rows)
    region_keys = df["regionkey"].tolist()
    return df, region_keys


def make_dim_product(n: int = 30):
    brands = ["Alpha", "Beta", "Gamma"]
    categories = ["Electronics", "Food", "Beauty"]
    subcats = {
        "Electronics": ["Phone", "Laptop", "Accessory"],
        "Food": ["Snack", "Drink"],
        "Beauty": ["Skincare", "Makeup"],
    }
    rows = []
    for i in range(1, n + 1):
        brand = random.choice(brands)
        cat = random.choice(categories)
        subcat = random.choice(subcats[cat])
        code = f"P{i:03d}"
        name = f"{brand} {cat} {i:03d}"
        rows.append(
            dict(
                productkey=i,
                productcode=code,
                productname=name,
                brand=brand,
                category=cat,
                subcategory=subcat,
                uom="PCS",
                isactive=True,
            )
        )
    df = pd.DataFrame(rows)
    product_keys = df["productkey"].tolist()
    return df, product_keys


def make_dim_customer(region_keys, n: int = 40):
    types = ["Distributor", "Retail", "Online"]
    segments = ["Gold", "Silver", "Bronze"]
    rows = []
    for i in range(1, n + 1):
        code = f"C{i:03d}"
        name = f"Customer {i:03d}"
        ctype = random.choice(types)
        seg = random.choice(segments)
        region = random.choice(region_keys)
        channel = random.choice(["B2B", "B2C"])
        rows.append(
            dict(
                customerkey=i,
                customercode=code,
                customername=name,
                customertype=ctype,
                customersegment=seg,
                regionkey=region,
                channel=channel,
                isactive=True,
            )
        )
    df = pd.DataFrame(rows)
    customer_keys = df["customerkey"].tolist()
    return df, customer_keys


def make_dim_warehouse(region_keys):
    rows = []
    for i in range(1, 4):
        code = f"WH{i:02d}"
        name = f"Main Warehouse {i}"
        region = random.choice(region_keys)
        rows.append(
            dict(
                warehousekey=i,
                warehousecode=code,
                warehousename=name,
                regionkey=region,
                isactive=True,
            )
        )
    df = pd.DataFrame(rows)
    wh_keys = df["warehousekey"].tolist()
    return df, wh_keys


def make_dim_salesrep(region_keys):
    rows = []
    for i in range(1, 15):
        empcode = f"SR{i:03d}"
        name = f"Sales Rep {i:03d}"
        region = random.choice(region_keys)
        email = f"sales{i:03d}@example.com"
        rows.append(
            dict(
                salesrepkey=i,
                employeecode=empcode,
                fullname=name,
                regionkey=region,
                email=email,
                isactive=True,
            )
        )
    df = pd.DataFrame(rows)
    salesrep_keys = df["salesrepkey"].tolist()
    return df, salesrep_keys


def make_dim_glaccount():
    base_rows = [
        ("4000", "Product Revenue", "PL", "Revenue", "Sales"),
        ("5000", "Cost of Goods Sold", "PL", "COGS", "COGS"),
        ("6100", "Operating Expenses", "PL", "OPEX", "OPEX"),
        ("1000", "Cash and Bank", "BS", "Assets", "Cash"),
        ("2000", "Trade Payables", "BS", "Liabilities", "Payables"),
        ("3000", "Share Capital", "BS", "Equity", "Equity"),
        ("7000", "Operating Cash Flow", "CF", "Operating", "CFO"),
        ("7100", "Investing Cash Flow", "CF", "Investing", "CFI"),
        ("7200", "Financing Cash Flow", "CF", "Financing", "CFF"),
    ]
    rows = []
    for i, (code, name, stype, cat, subcat) in enumerate(base_rows, start=1):
        rows.append(
            dict(
                glaccountkey=i,
                glaccountcode=code,
                glaccountname=name,
                statementtype=stype,
                category=cat,
                subcategory=subcat,
            )
        )
    df = pd.DataFrame(rows)
    pl_ids = df.loc[df["statementtype"] == "PL", "glaccountkey"].tolist()
    bs_ids = df.loc[df["statementtype"] == "BS", "glaccountkey"].tolist()
    cf_ids = df.loc[df["statementtype"] == "CF", "glaccountkey"].tolist()
    return df, pl_ids, bs_ids, cf_ids


# -------------------------------------------------------------------
# Facts
# -------------------------------------------------------------------
def make_fact_sales(
    date_keys, customer_keys, product_keys, region_keys, salesrep_keys, wh_keys
):
    rows = []
    for _ in range(4000):  # 4k sales lines
        datekey = random.choice(date_keys)
        cust = random.choice(customer_keys)
        prod = random.choice(product_keys)
        region = random.choice(region_keys)
        salesrep = random.choice(salesrep_keys)
        wh = random.choice(wh_keys)
        invoice_no = f"INV{random.randint(100000, 999999)}"
        line_no = random.randint(1, 5)
        qty = round(random.uniform(1, 50), 2)
        list_price = round(random.uniform(10, 200), 2)
        discount = round(list_price * qty * random.uniform(0, 0.2), 2)
        net_sales = round(list_price * qty - discount, 2)
        cogs = round(net_sales * random.uniform(0.5, 0.8), 2)
        gm = round(net_sales - cogs, 2)

        rows.append(
            dict(
                datekey=datekey,
                customerkey=cust,
                productkey=prod,
                regionkey=region,
                salesrepkey=salesrep,
                warehousekey=wh,
                invoicenumber=invoice_no,
                invoicelineno=line_no,
                quantity=qty,
                listprice=list_price,
                discountamount=discount,
                netsales=net_sales,
                cogs=cogs,
                grossmargin=gm,
                currency="EUR",
            )
        )

    return pd.DataFrame(rows)


def make_fact_sales_target(month_first_keys, region_keys):
    rows = []
    for mk in month_first_keys:
        for region in region_keys:
            revenue_target = round(random.uniform(20_000, 80_000), 2)
            qty_target = round(random.uniform(500, 3_000), 2)
            rows.append(
                dict(
                    datekey=mk,
                    regionkey=region,
                    salesrepkey=None,
                    productkey=None,
                    targetrevenue=revenue_target,
                    targetquantity=qty_target,
                )
            )
    return pd.DataFrame(rows)


def make_fact_orders(date_rows, customer_keys, product_keys, region_keys, wh_keys):
    rows = []
    for _ in range(2500):
        order_no = f"SO{random.randint(100000, 999999)}"
        line_no = random.randint(1, 5)

        order_date_key, order_date = random.choice(date_rows)

        cust = random.choice(customer_keys)
        prod = random.choice(product_keys)
        region = random.choice(region_keys)
        wh = random.choice(wh_keys)
        ordered_qty = round(random.uniform(1, 80), 2)

        req_offset = random.randint(1, 20)
        prom_delay = random.randint(0, 5)
        ship_delay = random.randint(-2, 7)

        requested_date = order_date + timedelta(days=req_offset)
        promised_date = requested_date + timedelta(days=prom_delay)
        actual_ship = promised_date + timedelta(days=ship_delay)

        shipped_qty = max(0, round(ordered_qty - random.uniform(0, 10), 2))
        cancelled_qty = max(0, round(ordered_qty - shipped_qty, 2))
        is_on_time = actual_ship <= promised_date
        is_in_full = shipped_qty >= ordered_qty * 0.98

        rows.append(
            dict(
                ordernumber=order_no,
                orderlineno=line_no,
                orderdatekey=order_date_key,
                customerkey=cust,
                productkey=prod,
                regionkey=region,
                warehousekey=wh,
                orderedqty=ordered_qty,
                requesteddeliverydate=requested_date,
                promiseddeliverydate=promised_date,
                actualshipdate=actual_ship,
                shippedqty=shipped_qty,
                cancelledqty=cancelled_qty,
                isontime=is_on_time,
                isinfull=is_in_full,
            )
        )
    return pd.DataFrame(rows)


def make_fact_inventory(date_rows, product_keys, wh_keys):
    # date_rows: list of (datekey, date)
    rows = []
    for dk, d in date_rows:
        next_day = d + timedelta(days=1)
        # only month-end snapshots
        if next_day.month == d.month:
            continue

        sample_products = random.sample(product_keys, k=min(10, len(product_keys)))
        for prod in sample_products:
            wh = random.choice(wh_keys)
            opening = round(random.uniform(0, 200), 2)
            inbound = round(random.uniform(0, 100), 2)
            outbound = round(random.uniform(0, 120), 2)
            closing = max(0, opening + inbound - outbound)
            value = round(closing * random.uniform(5, 40), 2)
            age = round(random.uniform(5, 120), 1)
            prov = round(value * random.uniform(0, 0.2), 2)

            rows.append(
                dict(
                    datekey=dk,
                    productkey=prod,
                    warehousekey=wh,
                    openingqty=opening,
                    inboundqty=inbound,
                    outboundqty=outbound,
                    closingqty=closing,
                    inventoryvalue=value,
                    averageagedays=age,
                    provisionamount=prov,
                )
            )
    return pd.DataFrame(rows)


def make_fact_production(date_rows, product_keys, wh_keys):
    rows = []
    for dk, d in date_rows:
        # weekday? (1–5)
        if d.isoweekday() > 5:
            continue

        sample_products = random.sample(product_keys, k=min(5, len(product_keys)))
        for prod in sample_products:
            wh = random.choice(wh_keys)
            produced = round(random.uniform(0, 150), 2)
            scrap = round(produced * random.uniform(0, 0.1), 2)
            machine_hours = round(random.uniform(1, 20), 2)
            downtime = round(random.uniform(0, 3), 2)

            rows.append(
                dict(
                    datekey=dk,
                    productkey=prod,
                    warehousekey=wh,
                    producedqty=produced,
                    scrapqty=scrap,
                    machinehours=machine_hours,
                    downtimehours=downtime,
                )
            )
    return pd.DataFrame(rows)


def make_fact_finance(month_first_keys, pl_ids, bs_ids, cf_ids, region_keys):
    # P&L monthly
    pl_rows = []
    for mk in month_first_keys:
        for gl in pl_ids:
            region = random.choice(region_keys + [None])
            amount = round(random.uniform(-80_000, 80_000), 2)
            pl_rows.append(
                dict(
                    datekey=mk,
                    glaccountkey=gl,
                    regionkey=region,
                    amount=amount,
                    currency="EUR",
                )
            )

    # Balance Sheet monthly balances
    bs_rows = []
    for mk in month_first_keys:
        for gl in bs_ids:
            region = random.choice(region_keys + [None])
            bal = round(random.uniform(-200_000, 200_000), 2)
            bs_rows.append(
                dict(
                    datekey=mk,
                    glaccountkey=gl,
                    regionkey=region,
                    balanceamount=bal,
                    currency="EUR",
                )
            )

    # Cash Flow
    cf_rows = []
    for mk in month_first_keys:
        for gl in cf_ids:
            cf = round(random.uniform(-100_000, 100_000), 2)
            cf_rows.append(
                dict(
                    datekey=mk,
                    glaccountkey=gl,
                    regionkey=None,
                    cashflowamount=cf,
                    currency="EUR",
                )
            )

    return (
        pd.DataFrame(pl_rows),
        pd.DataFrame(bs_rows),
        pd.DataFrame(cf_rows),
    )


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main():
    print("Generating mock CSV data (no database)…")

    # 1. Dates
    dim_date, date_rows, date_keys, month_first_keys = make_dim_date()

    # 2. Dimensions
    dim_region, region_keys = make_dim_region()
    dim_product, product_keys = make_dim_product()
    dim_customer, customer_keys = make_dim_customer(region_keys)
    dim_warehouse, wh_keys = make_dim_warehouse(region_keys)
    dim_salesrep, salesrep_keys = make_dim_salesrep(region_keys)
    dim_glaccount, pl_ids, bs_ids, cf_ids = make_dim_glaccount()

    # 3. Facts
    fact_sales = make_fact_sales(
        date_keys, customer_keys, product_keys, region_keys, salesrep_keys, wh_keys
    )
    fact_sales_target = make_fact_sales_target(month_first_keys, region_keys)
    fact_orders = make_fact_orders(
        date_rows, customer_keys, product_keys, region_keys, wh_keys
    )
    fact_inventory = make_fact_inventory(date_rows, product_keys, wh_keys)
    fact_production = make_fact_production(date_rows, product_keys, wh_keys)
    fact_pl, fact_bs, fact_cf = make_fact_finance(
        month_first_keys, pl_ids, bs_ids, cf_ids, region_keys
    )

    # 4. Save all to CSV
    save_csv(dim_date, "dimdate")
    save_csv(dim_region, "dimregion")
    save_csv(dim_customer, "dimcustomer")
    save_csv(dim_product, "dimproduct")
    save_csv(dim_warehouse, "dimwarehouse")
    save_csv(dim_salesrep, "dimsalesrep")
    save_csv(dim_glaccount, "dimglaccount")

    save_csv(fact_sales, "factsales")
    save_csv(fact_sales_target, "factsalestarget")
    save_csv(fact_orders, "factorders")
    save_csv(fact_inventory, "factinventory")
    save_csv(fact_production, "factproduction")
    save_csv(fact_pl, "factfinancepl")
    save_csv(fact_bs, "factfinancebs")
    save_csv(fact_cf, "factfinancecf")

    print("✅ CSV mock data generation completed.")


if __name__ == "__main__":
    main()
