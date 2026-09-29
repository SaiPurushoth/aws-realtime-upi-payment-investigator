PYTHON ?= python3
CDK ?= cdk

.PHONY: help install-dev test cdk-synth cdk-diff flink-package simulator dashboard historical-deploy demo-check demo-normal demo-failure demo-reset

help:
	@echo "install-dev    Install local development dependencies"
	@echo "test           Run dependency-free unit tests"
	@echo "cdk-synth      Synthesize CloudFormation locally"
	@echo "cdk-diff       Compare the stack with the current AWS environment"
	@echo "flink-package  Build the PyFlink Kinesis connector archive"
	@echo "simulator      Send one NORMAL synthetic transaction to AWS"
	@echo "dashboard      Run the local Streamlit dashboard"
	@echo "historical-deploy  Deploy native Kinesis Streaming Tables history"
	@echo "demo-check     Validate deployed demo readiness"
	@echo "demo-normal    Generate one synthetic normal payment"
	@echo "demo-failure   Generate one synthetic callback failure"
	@echo "demo-reset     Explain the non-destructive local reset"

install-dev:
	$(PYTHON) -m pip install -r requirements-dev.txt

test:
	$(PYTHON) -m unittest discover -s tests -v

cdk-synth:
	cd infra && $(CDK) synth

cdk-diff:
	cd infra && $(CDK) diff

flink-package:
	./flink/package.sh

simulator:
	$(PYTHON) simulator/producer.py --scenario normal

dashboard:
	$(PYTHON) -m streamlit run dashboard/app.py

historical-deploy:
	$(PYTHON) infra/historical/deploy.py

demo-check:
	./scripts/demo-check.sh

demo-normal:
	./scripts/demo-normal.sh

demo-failure:
	./scripts/demo-failure.sh

demo-reset:
	./scripts/demo-reset.sh
