# database/mock_data/generate_data.py

import random
from datetime import date, timedelta
import psycopg2

CONN_STR = "postgresql://neondb_owner:npg_35XuDGUeAfHV@ep-empty-sky-a9a996ke-pooler.gwc.azure.neon.tech/neondb?sslmode=require&channel_binding=require"  # e.g. postgresql://neondb_owner:xxx@ep-xxx.neon.tech/neondb?sslmode=require&channel_binding=require

SCHEMA = "analytics"


def get_conn():
    return psycopg2.connect(CONN_STR)


# ---------------------------
# Helper: generate DimDate
# ---------------------------
def seed_dim_date(cur, start=date(2024, 1, 1), end=date(2025, 12, 31)):
    print("Seeding DimDate...")
    cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.dimdate;")
    count = cur.fetchone()[0]
    if count > 0:
        print(f"  DimDate already has {count} rows, skip.")
        return

    d = start
    rows = []
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
            (
                date_key,
                d,
                year,
                quarter,
                month,
                month_name,
                day_of_month,
                day_of_week,
                day_name,
                is_weekday,
            )
        )
        d += timedelta(days=1)

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.dimdate (
            datekey, fulldate, year, quarter, month, monthname,
            dayofmonth, dayofweek, dayname, isweekday
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        rows,
    )
    print(f"  Inserted {len(rows)} DimDate rows.")


# ---------------------------
# Seed simple dimensions
# ---------------------------
def seed_dim_region(cur):
    print("Seeding DimRegion...")
    cur.execute(f"TRUNCATE {SCHEMA}.dimregion RESTART IDENTITY CASCADE;")
    regions = [
        ("FR", "France", "Europe", "Nice"),
        ("DE", "Germany", "Europe", "Munich"),
        ("SG", "Singapore", "APAC", "Singapore"),
        ("US", "United States", "North America", "New York"),
    ]
    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.dimregion
        (countrycode, countryname, regionname, cityname)
        VALUES (%s,%s,%s,%s)
        """,
        regions,
    )
    cur.execute(f"SELECT regionkey, countrycode FROM {SCHEMA}.dimregion;")
    rows = cur.fetchall()
    region_ids = [r[0] for r in rows]
    print(f"  Inserted {len(region_ids)} regions.")
    return region_ids


def seed_dim_product(cur, n=30):
    print("Seeding DimProduct...")
    cur.execute(f"TRUNCATE {SCHEMA}.dimproduct RESTART IDENTITY CASCADE;")
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
        rows.append((code, name, brand, cat, subcat, "PCS", True))

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.dimproduct
        (productcode, productname, brand, category, subcategory, uom, isactive)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        rows,
    )
    cur.execute(f"SELECT productkey FROM {SCHEMA}.dimproduct;")
    ids = [r[0] for r in cur.fetchall()]
    print(f"  Inserted {len(ids)} products.")
    return ids


def seed_dim_customer(cur, region_ids, n=40):
    print("Seeding DimCustomer...")
    cur.execute(f"TRUNCATE {SCHEMA}.dimcustomer RESTART IDENTITY CASCADE;")
    types = ["Distributor", "Retail", "Online"]
    segments = ["Gold", "Silver", "Bronze"]
    rows = []
    for i in range(1, n + 1):
        code = f"C{i:03d}"
        name = f"Customer {i:03d}"
        ctype = random.choice(types)
        seg = random.choice(segments)
        region = random.choice(region_ids)
        channel = random.choice(["B2B", "B2C"])
        rows.append((code, name, ctype, seg, region, channel, True))

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.dimcustomer
        (customercode, customername, customertype, customersegment,
         regionkey, channel, isactive)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        rows,
    )
    cur.execute(f"SELECT customerkey FROM {SCHEMA}.dimcustomer;")
    ids = [r[0] for r in cur.fetchall()]
    print(f"  Inserted {len(ids)} customers.")
    return ids


def seed_dim_warehouse(cur, region_ids):
    print("Seeding DimWarehouse...")
    cur.execute(f"TRUNCATE {SCHEMA}.dimwarehouse RESTART IDENTITY CASCADE;")
    rows = []
    for i in range(1, 4):
        code = f"WH{i:02d}"
        name = f"Main Warehouse {i}"
        region = random.choice(region_ids)
        rows.append((code, name, region, True))

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.dimwarehouse
        (warehousecode, warehousename, regionkey, isactive)
        VALUES (%s,%s,%s,%s)
        """,
        rows,
    )
    cur.execute(f"SELECT warehousekey FROM {SCHEMA}.dimwarehouse;")
    ids = [r[0] for r in cur.fetchall()]
    print(f"  Inserted {len(ids)} warehouses.")
    return ids


def seed_dim_salesrep(cur, region_ids):
    print("Seeding DimSalesRep...")
    cur.execute(f"TRUNCATE {SCHEMA}.dimsalesrep RESTART IDENTITY CASCADE;")
    rows = []
    for i in range(1, 15):
        empcode = f"SR{i:03d}"
        name = f"Sales Rep {i:03d}"
        region = random.choice(region_ids)
        email = f"sales{i:03d}@example.com"
        rows.append((empcode, name, region, email, True))

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.dimsalesrep
        (employeecode, fullname, regionkey, email, isactive)
        VALUES (%s,%s,%s,%s,%s)
        """,
        rows,
    )
    cur.execute(f"SELECT salesrepkey FROM {SCHEMA}.dimsalesrep;")
    ids = [r[0] for r in cur.fetchall()]
    print(f"  Inserted {len(ids)} sales reps.")
    return ids


def seed_dim_glaccount(cur):
    print("Seeding DimGLAccount...")
    cur.execute(f"TRUNCATE {SCHEMA}.dimglaccount RESTART IDENTITY CASCADE;")
    rows = [
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
    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.dimglaccount
        (glaccountcode, glaccountname, statementtype, category, subcategory)
        VALUES (%s,%s,%s,%s,%s)
        """,
        rows,
    )
    cur.execute(
        f"SELECT glaccountkey, glaccountcode, statementtype FROM {SCHEMA}.dimglaccount;"
    )
    rows = cur.fetchall()
    pl_ids = [r[0] for r in rows if r[2] == "PL"]
    bs_ids = [r[0] for r in rows if r[2] == "BS"]
    cf_ids = [r[0] for r in rows if r[2] == "CF"]
    print(f"  Inserted {len(rows)} GL accounts.")
    return pl_ids, bs_ids, cf_ids


# ---------------------------
# Seed fact tables
# ---------------------------
def reset_facts(cur):
    print("Truncating fact tables...")
    cur.execute(
        f"""
        TRUNCATE
          {SCHEMA}.factsales,
          {SCHEMA}.factsalestarget,
          {SCHEMA}.factorders,
          {SCHEMA}.factinventory,
          {SCHEMA}.factproduction,
          {SCHEMA}.factfinancepl,
          {SCHEMA}.factfinancebs,
          {SCHEMA}.factfinancecf
        RESTART IDENTITY CASCADE;
        """
    )


def seed_fact_sales(cur, date_keys, customer_ids, product_ids, region_ids, salesrep_ids, wh_ids):
    print("Seeding FactSales (this may take a little)...")
    rows = []
    for _ in range(4000):  # 4k sales lines
        datekey = random.choice(date_keys)
        cust = random.choice(customer_ids)
        prod = random.choice(product_ids)
        region = random.choice(region_ids)
        salesrep = random.choice(salesrep_ids)
        wh = random.choice(wh_ids)
        invoice_no = f"INV{random.randint(100000, 999999)}"
        line_no = random.randint(1, 5)
        qty = round(random.uniform(1, 50), 2)
        list_price = round(random.uniform(10, 200), 2)
        discount = round(list_price * qty * random.uniform(0, 0.2), 2)
        net_sales = round(list_price * qty - discount, 2)
        cogs = round(net_sales * random.uniform(0.5, 0.8), 2)
        gm = round(net_sales - cogs, 2)
        rows.append(
            (
                datekey,
                cust,
                prod,
                region,
                salesrep,
                wh,
                invoice_no,
                line_no,
                qty,
                list_price,
                discount,
                net_sales,
                cogs,
                gm,
                "EUR",
            )
        )

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.factsales
        (datekey, customerkey, productkey, regionkey, salesrepkey, warehousekey,
         invoicenumber, invoicelineno,
         quantity, listprice, discountamount, netsales, cogs, grossmargin, currency)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        rows,
    )
    print(f"  Inserted {len(rows)} FactSales rows.")


def seed_fact_sales_target(cur, month_first_keys, region_ids, salesrep_ids, product_ids):
    print("Seeding FactSalesTarget...")
    rows = []
    for mk in month_first_keys:
        for region in region_ids:
            revenue_target = round(random.uniform(20000, 80000), 2)
            qty_target = round(random.uniform(500, 3000), 2)
            # 简化：不细分 salesrep/product，只按 region+month
            rows.append((mk, region, None, None, revenue_target, qty_target))

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.factsalestarget
        (datekey, regionkey, salesrepkey, productkey,
         targetrevenue, targetquantity)
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        rows,
    )
    print(f"  Inserted {len(rows)} FactSalesTarget rows.")


def seed_fact_orders(cur, date_keys, customer_ids, product_ids, region_ids, wh_ids):
    print("Seeding FactOrders...")
    rows = []
    for _ in range(2500):
        order_no = f"SO{random.randint(100000, 999999)}"
        line_no = random.randint(1, 5)
        order_date = random.choice(date_keys)
        cust = random.choice(customer_ids)
        prod = random.choice(product_ids)
        region = random.choice(region_ids)
        wh = random.choice(wh_ids)
        ordered_qty = round(random.uniform(1, 80), 2)

        req_offset = random.randint(1, 20)
        prom_delay = random.randint(0, 5)
        ship_delay = random.randint(-2, 7)
        requested_date = order_date + req_offset
        promised_date = requested_date + prom_delay
        actual_ship = promised_date + ship_delay

        shipped_qty = max(0, round(ordered_qty - random.uniform(0, 10), 2))
        cancelled_qty = max(0, round(ordered_qty - shipped_qty, 2))
        is_on_time = actual_ship <= promised_date
        is_in_full = shipped_qty >= ordered_qty * 0.98

        rows.append(
            (
                order_no,
                line_no,
                order_date,
                cust,
                prod,
                region,
                wh,
                ordered_qty,
                requested_date,
                promised_date,
                actual_ship,
                shipped_qty,
                cancelled_qty,
                is_on_time,
                is_in_full,
            )
        )

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.factorders
        (ordernumber, orderlineno, orderdatekey,
         customerkey, productkey, regionkey, warehousekey,
         orderedqty, requesteddeliverydate, promiseddeliverydate,
         actualshipdate, shippedqty, cancelledqty,
         isontime, isinfull)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        rows,
    )
    print(f"  Inserted {len(rows)} FactOrders rows.")


def seed_fact_inventory(cur, date_keys, product_ids, wh_ids):
    print("Seeding FactInventory...")
    rows = []
    # 只在每月月末做 snapshot，减少行数
    for dk in date_keys:
        # 取 fulldate，看是不是月末
        cur.execute(
            f"SELECT fulldate FROM {SCHEMA}.dimdate WHERE datekey = %s;", (dk,)
        )
        d = cur.fetchone()[0]
        next_day = d + timedelta(days=1)
        if next_day.month == d.month:  # 不是月末
            continue
        for prod in random.sample(product_ids, k=min(10, len(product_ids))):
            wh = random.choice(wh_ids)
            opening = round(random.uniform(0, 200), 2)
            inbound = round(random.uniform(0, 100), 2)
            outbound = round(random.uniform(0, 120), 2)
            closing = max(0, opening + inbound - outbound)
            value = round(closing * random.uniform(5, 40), 2)
            age = round(random.uniform(5, 120), 1)
            prov = round(value * random.uniform(0, 0.2), 2)
            rows.append(
                (dk, prod, wh, opening, inbound, outbound, closing, value, age, prov)
            )

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.factinventory
        (datekey, productkey, warehousekey,
         openingqty, inboundqty, outboundqty, closingqty,
         inventoryvalue, averageagedays, provisionamount)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        rows,
    )
    print(f"  Inserted {len(rows)} FactInventory rows.")


def seed_fact_production(cur, date_keys, product_ids, wh_ids):
    print("Seeding FactProduction...")
    rows = []
    for dk in date_keys:
        # 只选工作日
        cur.execute(
            f"SELECT isweekday FROM {SCHEMA}.dimdate WHERE datekey = %s;", (dk,)
        )
        if not cur.fetchone()[0]:
            continue
        for prod in random.sample(product_ids, k=min(5, len(product_ids))):
            wh = random.choice(wh_ids)
            produced = round(random.uniform(0, 150), 2)
            scrap = round(produced * random.uniform(0, 0.1), 2)
            machine_hours = round(random.uniform(1, 20), 2)
            downtime = round(random.uniform(0, 3), 2)
            rows.append((dk, prod, wh, produced, scrap, machine_hours, downtime))

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.factproduction
        (datekey, productkey, warehousekey,
         producedqty, scrapqty, machinehours, downtimehours)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        rows,
    )
    print(f"  Inserted {len(rows)} FactProduction rows.")


def seed_fact_finance(cur, month_first_keys, pl_ids, bs_ids, cf_ids, region_ids):
    print("Seeding Finance facts (PL/BS/CF)...")
    # P&L 按月
    pl_rows = []
    for mk in month_first_keys:
        for gl in pl_ids:
            region = random.choice(region_ids + [None])
            amount = round(random.uniform(-80000, 80000), 2)
            pl_rows.append((mk, gl, region, amount, "EUR"))

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.factfinancepl
        (datekey, glaccountkey, regionkey, amount, currency)
        VALUES (%s,%s,%s,%s,%s)
        """,
        pl_rows,
    )

    # Balance Sheet：期末余额
    bs_rows = []
    for mk in month_first_keys:
        for gl in bs_ids:
            region = random.choice(region_ids + [None])
            bal = round(random.uniform(-200000, 200000), 2)
            bs_rows.append((mk, gl, region, bal, "EUR"))

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.factfinancebs
        (datekey, glaccountkey, regionkey, balanceamount, currency)
        VALUES (%s,%s,%s,%s,%s)
        """,
        bs_rows,
    )

    # Cash Flow
    cf_rows = []
    for mk in month_first_keys:
        for gl in cf_ids:
            region = None
            cf = round(random.uniform(-100000, 100000), 2)
            cf_rows.append((mk, gl, region, cf, "EUR"))

    cur.executemany(
        f"""
        INSERT INTO {SCHEMA}.factfinancecf
        (datekey, glaccountkey, regionkey, cashflowamount, currency)
        VALUES (%s,%s,%s,%s,%s)
        """,
        cf_rows,
    )

    print(
        f"  Inserted {len(pl_rows)} PL, {len(bs_rows)} BS, {len(cf_rows)} CF rows."
    )


# ---------------------------
# Main
# ---------------------------
def main():
    conn = get_conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()

        # 1. DimDate
        seed_dim_date(cur)

        # 准备所有 datekey / 月初 datekey
        cur.execute(
            f"SELECT datekey, fulldate FROM {SCHEMA}.dimdate ORDER BY fulldate;"
        )
        date_rows = cur.fetchall()
        date_keys = [r[0] for r in date_rows]

        month_first_keys = sorted(
            {
                int(r[1].replace(day=1).strftime("%Y%m%d"))
                for r in date_rows
            }
        )

        # 2. Dimensions
        region_ids = seed_dim_region(cur)
        product_ids = seed_dim_product(cur)
        customer_ids = seed_dim_customer(cur, region_ids)
        wh_ids = seed_dim_warehouse(cur, region_ids)
        salesrep_ids = seed_dim_salesrep(cur, region_ids)
        pl_ids, bs_ids, cf_ids = seed_dim_glaccount(cur)

        # 3. Facts
        reset_facts(cur)
        seed_fact_sales(cur, date_keys, customer_ids, product_ids, region_ids, salesrep_ids, wh_ids)
        seed_fact_sales_target(cur, month_first_keys, region_ids, salesrep_ids, product_ids)
        seed_fact_orders(cur, date_keys, customer_ids, product_ids, region_ids, wh_ids)
        seed_fact_inventory(cur, date_keys, product_ids, wh_ids)
        seed_fact_production(cur, date_keys, product_ids, wh_ids)
        seed_fact_finance(cur, month_first_keys, pl_ids, bs_ids, cf_ids, region_ids)

        conn.commit()
        print("✅ Mock data generation completed.")
    except Exception as e:
        conn.rollback()
        print("❌ Error, rolled back:", e)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

