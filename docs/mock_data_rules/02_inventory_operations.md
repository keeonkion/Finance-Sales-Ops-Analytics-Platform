Mock Data Design — Inventory and Operations

This document defines the rules used to simulate Inventory, Warehouse, Production, and Fulfillment operations.

1. Inventory Snapshot Rules
1.1 Monthly Snapshot

Inventory data is generated once per month (month-end).

1.2 Metrics

OpeningQty

InboundQty

OutboundQty

ClosingQty (calculated)

InventoryValue = ClosingQty × (unit cost 5–40 EUR)

1.3 Provision Rules

Provision rate: 0–20%

NetInventory = InventoryValue – ProvisionAmount

2. Production Rules

Production data is only generated on working days (Mon–Fri).

2.1 Key Metrics

ProductionQty: 0–150 units

ScrapRate: 0–10%

MachineHours: 1–20 hours

DowntimeHours: 0–3 hours

3. Purchase Order / Sales Order Operational Rules
3.1 Lead Time

Lead time: 1–20 days

3.2 Promised vs Actual Delivery

PromisedDate = RequestDate + LeadTime

ActualShipDate = PromisedDate ± random delay

3.3 OTIF (On-Time-In-Full) Logic

OnTime = ActualShipDate ≤ PromisedDate

InFull = ShippedQty ≥ 98% of OrderedQty

OTIF = OnTime AND InFull

4. Warehouse Assignment

Products are stored in 1 of 3 warehouses.

Each warehouse belongs to a region defined in DimRegion.
