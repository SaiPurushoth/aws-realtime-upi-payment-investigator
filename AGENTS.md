# Repository guidance

## Non-negotiable scope

- Treat all payment data as synthetic demo data.
- Never add connections to real UPI, NPCI, BHIM, banks, PSPs, or payment
  infrastructure.
- Never include real-looking credentials, account identifiers, phone numbers,
  or production endpoints in examples or fixtures.
- Do not deploy, bootstrap, upload, or mutate AWS resources unless the user
  explicitly asks for that action in a later request.

## Engineering constraints

- Use Python 3.11.
- Keep components small and readable for a live meetup walkthrough.
- Keep AWS calls at adapter boundaries so core logic remains unit-testable.
- Use least-privilege IAM grants and environment-based configuration.
- The Streamlit dashboard is read-only and only visualizes AWS data.
- Add or update unit tests whenever behavior changes.

## Verification

Run `make test` before handing off changes. `make cdk-synth` is allowed because
it is local-only; `cdk deploy` is not allowed without an explicit user request.
