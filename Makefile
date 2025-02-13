## Tool Versions
KAWKEYE_VERSION ?= v6.0.0

.PHONY: install-hawkeye
install-hawkeye: ## Install hawkeye.
	curl --proto '=https' --tlsv1.2 -LsSf https://github.com/korandoru/hawkeye/releases/download/${KAWKEYE_VERSION}/hawkeye-installer.sh | sh

.PHONY: check-lincense-header
check-lincense-header: install-hawkeye ## Check License Header.
	hawkeye check --config licenserc.toml

.PHONY: format-lincense-header
format-lincense-header: install-hawkeye ## Format License Header.
	hawkeye format --config licenserc.toml

.PHONY: remove-lincense-header
remove-lincense-header: install-hawkeye ## Remove License Header.
	hawkeye remove --config licenserc.toml
