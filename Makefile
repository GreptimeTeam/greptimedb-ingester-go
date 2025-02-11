## Tool Versions
KAWKEYE_VERSION ?= v6.0.0

.PHONY: hawkeye
hawkeye: ## Install hawkeye.
	curl --proto '=https' --tlsv1.2 -LsSf https://github.com/korandoru/hawkeye/releases/download/${KAWKEYE_VERSION}/hawkeye-installer.sh | sh

.PHONY: check-lincense-header
check-lincense-header: ## Check License Header.
	hawkeye check --config licenserc.toml

.PHONY: format-lincense-header
format-lincense-header: ## Format License Header.
	hawkeye format --config licenserc.toml

.PHONY: remove-lincense-header
remove-lincense-header: ## Remove License Header.
	hawkeye remove --config licenserc.toml
