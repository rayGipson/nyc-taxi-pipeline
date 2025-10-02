-- Seed reference data for dimensions

INSERT INTO warehouse.dim_vendor (vendor_id, vendor_name) VALUES
    (1, 'Creative Mobile Technologies'),
    (2, 'VeriFone Inc.')
ON CONFLICT (vendor_id) DO NOTHING;

INSERT INTO warehouse.dim_rate_code (rate_code_id, rate_code_name) VALUES
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride')
ON CONFLICT (rate_code_id) DO NOTHING;

INSERT INTO warehouse.dim_payment_type (payment_type_id, payment_type_name) VALUES
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip')
ON CONFLICT (payment_type_id) DO NOTHING;
