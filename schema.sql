-- =============================================================================
-- schema.sql
-- Data Infrastructure & Analytics Engineering
-- Author: Tauseef Ahmad Khan
-- Relational schema for manufacturing supply chain analytics
-- Compatible with MySQL 8.0+
-- =============================================================================

-- ── DATABASE SETUP ─────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS supply_chain_analytics;
USE supply_chain_analytics;

-- ── TABLE: suppliers ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id     VARCHAR(10)    PRIMARY KEY,
    supplier_name   VARCHAR(100)   NOT NULL,
    country         VARCHAR(50),
    tier            ENUM('Tier 1', 'Tier 2', 'Tier 3') DEFAULT 'Tier 2',
    on_time_rate    DECIMAL(5,3),
    quality_score   DECIMAL(3,2),
    avg_lead_days   INT,
    contract_value  DECIMAL(12,2),
    active          TINYINT(1)     DEFAULT 1,
    created_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);

-- ── TABLE: orders ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    order_id        VARCHAR(12)    PRIMARY KEY,
    order_date      DATE           NOT NULL,
    delivery_date   DATE,
    customer        VARCHAR(100),
    product         VARCHAR(100),
    plant           VARCHAR(50),
    supplier_id     VARCHAR(10),
    quantity        INT,
    unit_price      DECIMAL(10,2),
    discount_pct    DECIMAL(5,3)   DEFAULT 0,
    revenue         DECIMAL(14,2),
    status          ENUM('Delivered','Pending','Cancelled') DEFAULT 'Pending',
    created_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- ── TABLE: quality_inspections ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS quality_inspections (
    inspection_id   VARCHAR(12)    PRIMARY KEY,
    order_id        VARCHAR(12),
    product         VARCHAR(100),
    plant           VARCHAR(50),
    inspection_date DATE,
    batch_size      INT,
    defects_found   INT            DEFAULT 0,
    defect_type     VARCHAR(50),
    defect_rate     DECIMAL(6,4)   DEFAULT 0,
    inspector_id    VARCHAR(15),
    passed          TINYINT(1)     DEFAULT 1,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- ── INDEXES for query performance ──────────────────────────────────────────
CREATE INDEX idx_orders_customer    ON orders(customer);
CREATE INDEX idx_orders_date        ON orders(order_date);
CREATE INDEX idx_orders_status      ON orders(status);
CREATE INDEX idx_orders_supplier    ON orders(supplier_id);
CREATE INDEX idx_quality_product    ON quality_inspections(product);
CREATE INDEX idx_quality_plant      ON quality_inspections(plant);
CREATE INDEX idx_quality_defect     ON quality_inspections(defect_type);
