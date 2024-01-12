// Copyright 2024 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"bytes"
	"encoding/binary"

	"github.com/bits-and-blooms/bitset"
)

// Mask is help to set null bits.
type Mask struct {
	bs bitset.BitSet
}

// set is to set which position is to be set to 1
func (n *Mask) Set(idx uint) *Mask {
	n.bs.Set(idx)
	return n
}

// shrink is to help to generate the bytes number the caller is interested
// via LittleEndian
func (n *Mask) Shrink(bSize int) ([]byte, error) {
	if n.bs.Len() == 0 {
		return nil, nil
	}

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, n.bs.Bytes())
	if err != nil {
		return nil, err
	}
	return buf.Bytes()[:bSize], nil
}
