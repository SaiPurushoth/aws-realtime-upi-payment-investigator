# UPI Payment Investigator runtime

This is the CodeZip package for the synthetic `upi-payment-investigator` Strands
agent. It accepts `incident_id`, `transaction_id`, and `incident_type`, reads one
synthetic transaction from DynamoDB, and returns strict investigation JSON.

It does not connect to real UPI, NPCI, BHIM, bank, or payment infrastructure.
