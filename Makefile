# Copyright 2025 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## Tool Versions
KAWKEYE_VERSION ?= v6.5.1
GOLANGCI_LINT_VERSION ?= v2.1.6

.PHONY: hawkeye
hawkeye: ## Install hawkeye.
	curl --proto '=https' --tlsv1.2 -LsSf https://github.com/korandoru/hawkeye/releases/download/${KAWKEYE_VERSION}/hawkeye-installer.sh | sh

.PHONY: check-license
check-license: hawkeye ## Check License Header.
	hawkeye check --config licenserc.toml

.PHONY: format-license
format-license: hawkeye ## Format License Header.
	hawkeye format --config licenserc.toml --fail-if-updated false

.PHONY: remove-license
remove-license: ## Remove License Header.
	hawkeye remove --config licenserc.toml

.PHONY: lint
lint: golangci-lint ## Run lint.
	golangci-lint run -v ./...

.PHONY: golangci-lint
golangci-lint: ## Install golangci-lint.
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}
