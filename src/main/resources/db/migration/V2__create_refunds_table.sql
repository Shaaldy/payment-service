CREATE TABLE refunds(
                        id UUID PRIMARY KEY,
                        payment_id UUID NOT NULL UNIQUE REFERENCES payments(id),
                        amount DECIMAL(10, 2) NOT NULL CHECK (amount >= 0),
                        created_at TIMESTAMP NOT NULL DEFAULT NOW()
)