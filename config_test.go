/*
 * Copyright 2026 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package greptime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GreptimeTeam/greptimedb-ingester-go/loadbalancer"
)

func TestNewConfigNoArgs(t *testing.T) {
	cfg := NewConfig()
	assert.Equal(t, "", cfg.Host)
	assert.Equal(t, 4001, cfg.Port)
	assert.Empty(t, cfg.resolveEndpoints(),
		"no host and no endpoints should resolve to nothing so NewClient can report a clear error")
}

func TestNewConfigSingleHostLegacyForm(t *testing.T) {
	cfg := NewConfig("greptimedb.example.com")
	assert.Equal(t, "greptimedb.example.com", cfg.Host)
	assert.Equal(t, 4001, cfg.Port)
	assert.Equal(t, []string{"greptimedb.example.com:4001"}, cfg.resolveEndpoints())
}

func TestNewConfigSingleHostRespectsWithPort(t *testing.T) {
	cfg := NewConfig("h").WithPort(14001)
	assert.Equal(t, []string{"h:14001"}, cfg.resolveEndpoints())
}

func TestNewConfigSingleArgIPv6BareHost(t *testing.T) {
	cfg := NewConfig("[::1]")
	assert.Equal(t, "[::1]", cfg.Host,
		"bracketed IPv6 without a port must stay in the legacy bare-host form")
	assert.Equal(t, []string{"[::1]:4001"}, cfg.resolveEndpoints())
}

func TestNewConfigSingleArgIPv6HostPortShorthand(t *testing.T) {
	cfg := NewConfig("[::1]:4001")
	assert.Equal(t, "", cfg.Host, "host:port arg must not populate Host")
	assert.Equal(t, []string{"[::1]:4001"}, cfg.resolveEndpoints())
}

func TestNewConfigSingleArgHostPortShorthand(t *testing.T) {
	cfg := NewConfig("h1:5001")
	assert.Equal(t, "", cfg.Host, "host:port arg must not populate Host")
	assert.Equal(t, []string{"h1:5001"}, cfg.resolveEndpoints(),
		"single host:port arg must behave like WithEndpoints, not Host+Port concatenation")
}

func TestNewConfigSingleArgHostPortIgnoresWithPort(t *testing.T) {
	cfg := NewConfig("h1:5001").WithPort(9999)
	assert.Equal(t, []string{"h1:5001"}, cfg.resolveEndpoints(),
		"WithPort must not override an explicit host:port endpoint")
}

func TestNewConfigMultiArgShorthand(t *testing.T) {
	cfg := NewConfig("h1:4001", "h2:4001", "h3:4001")
	assert.Equal(t, "", cfg.Host, "multi-arg form must not set Host")
	assert.Equal(t, []string{"h1:4001", "h2:4001", "h3:4001"}, cfg.resolveEndpoints())
}

func TestNewConfigMultiArgEquivalentToWithEndpoints(t *testing.T) {
	a := NewConfig("h1:4001", "h2:4001")
	b := NewConfig().WithEndpoints("h1:4001", "h2:4001")
	assert.Equal(t, a.resolveEndpoints(), b.resolveEndpoints())
}

func TestWithEndpointsOverridesHostPort(t *testing.T) {
	cfg := NewConfig("legacy").WithPort(9999).WithEndpoints("h1:4001", "h2:4001")
	assert.Equal(t, []string{"h1:4001", "h2:4001"}, cfg.resolveEndpoints(),
		"explicit endpoints must win over Host:Port")
}

func TestWithEndpointsEmptyCallIsNoop(t *testing.T) {
	cfg := NewConfig("h1").WithEndpoints()
	assert.Equal(t, []string{"h1:4001"}, cfg.resolveEndpoints(),
		"WithEndpoints() with no args must not clobber a previously configured host")
}

func TestWithEndpointsCopiesSlice(t *testing.T) {
	input := []string{"h1:4001", "h2:4001"}
	cfg := NewConfig().WithEndpoints(input...)
	input[0] = "mutated:0"
	assert.Equal(t, []string{"h1:4001", "h2:4001"}, cfg.resolveEndpoints(),
		"mutating the caller's slice must not affect Config")
}

func TestWithLoadBalancerStoresPicker(t *testing.T) {
	picker := loadbalancer.NewRoundRobin()
	cfg := NewConfig("h").WithLoadBalancer(picker)
	assert.Same(t, picker, cfg.picker)
}

func TestNewClientNoEndpointConfigured(t *testing.T) {
	_, err := NewClient(NewConfig())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no endpoint configured")
}

func TestNewClientInvalidEndpoint(t *testing.T) {
	_, err := NewClient(NewConfig().WithEndpoints("missing-port"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid endpoint")
}

func TestNewClientWithoutPickerLeavesCfgUntouched(t *testing.T) {
	// grpc.NewClient is non-blocking so this succeeds without a real server.
	c, err := NewClient(NewConfig().WithEndpoints("127.0.0.1:1", "127.0.0.1:2"))
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	// NewClient must not mutate cfg.picker when substituting the default;
	// behavior of the default picker is covered in loadbalancer tests.
	assert.Nil(t, c.cfg.picker, "cfg.picker should be untouched; NewClient picks the default internally")
	assert.Len(t, c.pool.Addrs(), 2)
}
