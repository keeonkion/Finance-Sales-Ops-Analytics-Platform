Mock Data Design — Sales and Customer

This document defines the business rules used to generate mock Sales and Customer data for the Finance–Sales–Operations Analytics Platform.

1. Date Range

Sales data will be generated between 2024-01-01 and 2025-12-31.

All dates reference the DimDate table to ensure consistency across the model.

2. Sales Transaction Rules
2.1 Price & Quantity

Unit Price: EUR 10–200

Quantity per order line: 1–50 units

2.2 Discount & Margin

Discount: 0–20%

Gross Margin: 50–80%

2.3 Currency

Default currency: EUR

(Future enhancement: Multi-currency support)

3. Customer Segmentation Rules
3.1 Customer Type

Distributor

Retail

Online

3.2 Customer Segment

Gold

Silver

Bronze

3.3 Regional Assignment

Customers will be randomly assigned to regions from the DimRegion table.

4. Sales Rep Assignment

Each customer is mapped to a sales representative based on region.

Sales reps are pre-defined in DimSalesRep.

5. Additional Business Logic

Probability-based variation in order volume by weekday/month.

Seasonal spikes (e.g., 20% more orders in November–December).

5–10% of orders include discounts.
