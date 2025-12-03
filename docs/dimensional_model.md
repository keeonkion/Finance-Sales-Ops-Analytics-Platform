# Conceptual Dimensional Model – Finance, Sales & Operations Analytics Platform

This document defines the conceptual dimensional model for the unified Finance–Sales–Operations analytics platform.  
It includes the list of fact tables, dimension tables, and the rough star-schema diagram.

---

## 1. Overview

The analytics platform is designed around three business domains:

1. **Finance** – Profitability, cost structure, cash flow
2. **Sales** – Customers, revenue, discounts, targets
3. **Operations** – Inventory, production, delivery performance

Each domain contributes its own fact tables, connected through shared dimensions for consistency and cross-domain analysis.

---

## 2. Dimension Tables (Shared Across All Facts)

| Dimension Table | Description |
|------------------|-------------|
| **DimDate** | Standard date dimension (Day, Month, Quarter, Year, Fiscal periods) |
| **DimProduct** | Product-level attributes (Category, Brand, SKU, Line) |
| **DimCustomer** | Customer segmentation (Region, Tier, Industry, Channel) |
| **DimRegion** | Geographic hierarchy (Country → Region → City) |
| **DimSalesRep** | Sales representative / account owner information |
| **DimWarehouse** | Warehouse / storage / location for inventory |
| **DimGLAccount** | Finance GL mapping for P&L, BS, CF classifications (optional but recommended) |

---

## 3. Fact Tables by Business Area

### **3.1 Finance Facts**

| Fact Table | Description |
|------------|-------------|
| **FactFinancePL** | Profit & Loss metrics (Revenue, COGS, OPEX, Operating Profit, Net Profit) |
| **FactFinanceBS** | Balance sheet positions (Inventory value, AR, AP, Assets, Liabilities) |
| **FactFinanceCF** | Cash flow movements (Operating, Investing, Financing cash flows) |

---

### **3.2 Sales Facts**

| Fact Table | Description |
|------------|-------------|
| **FactSales** | Sales transactions with revenue, units sold, discount, cost, GM% |
| **FactSalesTarget** | Monthly / quarterly targets by region / sales rep / product |

---

### **3.3 Operations Facts**

| Fact Table | Description |
|------------|-------------|
| **FactInventory** | On-hand inventory levels, value, age, provision, turnover metrics |
| **FactInventoryMovement** | Inbound/outbound inventory movements (optional) |
| **FactOrders** | Order fulfilment, OTIF (on-time in-full), delays, cancellations |
| **FactProduction** | Production output, downtime, capacity utilization |

---

## 4. Shared Keys (Grain Definition)

All fact tables share the following keys where applicable:

- **DateKey**  
- **ProductKey**  
- **CustomerKey**  
- **RegionKey**  
- **WarehouseKey**  
- **SalesRepKey**  
- Additional domain-specific keys (e.g., GLAccountKey)

This ensures all visuals in Power BI can slice consistently across domains.

---

## 5. Rough Star Schema Diagram (Text Version)

                  ┌──────────────┐
                  │   DimDate    │
                  └──────┬───────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
   ┌──────┴──────┐ ┌─────┴─────┐ ┌──────┴──────┐
   │ FactSales   │ │FactFinance│ │FactInventory│
   └──────┬──────┘ └────┬──────┘ └──────┬──────┘
          │              │              │
    ──────┴─────────┐ ┌──┴────┐    ─────┴─────────┐
    │ DimProduct    │ │ DimGL │    │ DimWarehouse │
    └───────────────┘ └───────┘    └──────────────┘
    
    ┌────────────────────────────┐
    │ DimCustomer │
    └────────────────────────────┘
    
    ┌────────────────────────────┐
    │ DimRegion │
    └────────────────────────────┘
    
    ┌────────────────────────────┐
    │ DimSalesRep │
    └────────────────────────────┘


---

## 6. Diagram to Draw (for draw.io / Excalidraw)

Create a diagram with:

- **Center**:  
  - FactSales  
  - FactFinancePL / BS / CF  
  - FactInventory  
  - FactOrders  

- **Surrounding Dimensions**:  
  - DimDate  
  - DimProduct  
  - DimCustomer  
  - DimRegion  
  - DimWarehouse  
  - DimSalesRep  
  - DimGLAccount  

This will serve as the conceptual star schema for the project.

---

## 7. Next Steps (Technical)

This conceptual model will be used to:

1. Build PostgreSQL DDL scripts (Phase 1 - Task 4)  
2. Generate mock data (Phase 1 - Task 5)  
3. Construct the Power BI data model (Phase 2)  
4. Build pipelines (Airflow + dbt) (Phase 3)  
5. Publish documentation in Wiki

---

    
