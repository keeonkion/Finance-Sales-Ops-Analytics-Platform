# KPI List – Finance, Sales and Operations

This document defines the core KPIs required for the 60-Day Finance, Sales & Operations Analytics Platform.  
KPIs are mapped directly to key business questions from the Business Story.

---

## 1. Finance KPIs

| Business Question | KPI Name | Definition | Calculation Logic | Time Grain | Dimensions | Owner |
|------------------|----------|------------|-------------------|------------|------------|-------|
| Why is profitability not improving? | Operating Margin | Percentage of revenue remaining after operating expenses | Operating Profit / Revenue | Month | Region, Product Line, Country | CFO |
| Gross profit issue or expense issue? | Gross Margin | Profit after direct costs | (Revenue – COGS) / Revenue | Month | Region, Product, Customer Segment | CFO |
| Is cash flow enough for the next 6–12 months? | Operating Cash Flow Ratio | Ability to cover short-term liabilities with OCF | Operating Cash Flow / Short-term Liabilities | Quarter | Region, Business Unit | CFO |

---

## 2. Sales KPIs

| Business Question | KPI Name | Definition | Calculation Logic | Time Grain | Dimensions | Owner |
|-------------------|-----------|------------|-------------------|------------|------------|--------|
| Which customers bring 80% of revenue? | Top Customer Revenue Share | Share of revenue from top N customers | Revenue (Top N Customers) / Total Revenue | Month | Customer, Region, Product Line | Sales Director |
| Are discounts killing margin? | Discount Impact on Margin | Margin difference before vs after discount | Gross Margin (List Price) – Gross Margin (Actual Price) | Month | Customer, Product, Region | Sales Director |
| Are targets being achieved? | Sales Target Achievement | Actual revenue vs target | Actual Revenue / Target Revenue | Month | Region, Sales Rep, Product Category | Sales Director |

---

## 3. Operations KPIs

| Business Question | KPI Name | Definition | Calculation Logic | Time Grain | Dimensions | Owner |
|------------------|--------------|------------|-------------------|------------|------------|--------|
| Is inventory healthy? | Inventory Turnover | How many times inventory is sold and replaced per year | COGS / Average Inventory | Month | Product, Warehouse, Region | Operations Director |
| Any slow-moving or dead stock? | Slow-moving Inventory % | Inventory older than X days | Inventory (Age > X days) / Total Inventory | Month | Product, Warehouse | Operations Director |
| Are deliveries reliable? | On-time In-full (OTIF) | Orders delivered on time & in full | OTIF Orders / Total Orders | Week/Month | Customer, Region, Product | Operations Director |

---

## 4. North Star KPIs (Executive Dashboard)

These top-level KPIs drive company performance and will appear prominently in the homepage dashboard:

1. **Operating Margin** – Core measure of profitability  
2. **Sales Target Achievement** – Indicator of growth health  
3. **Inventory Turnover** – Measures cash efficiency and stock health  
4. **OTIF** – Proxy for supply chain reliability and customer experience  
5. **Operating Cash Flow Ratio** – Determines short-term liquidity strength

---
