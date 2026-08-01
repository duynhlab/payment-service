DROP TABLE IF EXISTS payment_attempts;

ALTER TABLE refunds
    DROP CONSTRAINT IF EXISTS refunds_status_check;
ALTER TABLE refunds
    ADD CONSTRAINT refunds_status_check CHECK (status IN ('pending','succeeded','failed'));

ALTER TABLE payments
    DROP CONSTRAINT IF EXISTS payments_status_check;
ALTER TABLE payments
    ADD CONSTRAINT payments_status_check CHECK (status IN
        ('pending','authorized','captured','failed','voided','expired','refunded'));
