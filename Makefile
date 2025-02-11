#    Copyright 2025 Greptime Team
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

## Tool Versions
KAW_KEYE_VERSION ?= v6.0.0

.PHONY: hawkeye
hawkeye: ## Install hawkeye.
	curl --proto '=https' --tlsv1.2 -LsSf https://github.com/korandoru/hawkeye/releases/download/${KAW_KEYE_VERSION}/hawkeye-installer.sh | sh

.PHONY: check-lincense
check-lincense: ## Check License Header.
	hawkeye check

.PHONY: format-lincense
format-lincense: ## Format License Header.
	hawkeye format

.PHONY: remove-lincense
remove-lincense: ## Remove License Header.
	hawkeye remove
