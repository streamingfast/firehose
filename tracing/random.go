// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracing

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"sync"
	"sync/atomic"
)

// Copied straight from https://github.com/census-instrumentation/opencensus-go/blob/master/trace/trace.go#L534..L598
//
// With slight modifications to only keep stuff around ID generation
// since it's not exposed publicly, this implementation is good enough
// anyway.

var config atomic.Value // access atomically

func init() {
	gen := &defaultIDGenerator{}
	// initialize traceID and spanID generators.
	var rngSeed int64
	for _, p := range []interface{}{
		&rngSeed, &gen.traceIDAdd, &gen.nextSpanID, &gen.spanIDInc,
	} {
		binary.Read(crand.Reader, binary.LittleEndian, p)
	}
	gen.traceIDRand = rand.New(rand.NewSource(rngSeed))
	gen.spanIDInc |= 1

	config.Store(gen)
}

type defaultIDGenerator struct {
	sync.Mutex

	// Please keep these as the first fields
	// so that these 8 byte fields will be aligned on addresses
	// divisible by 8, on both 32-bit and 64-bit machines when
	// performing atomic increments and accesses.
	// See:
	// * https://github.com/census-instrumentation/opencensus-go/issues/587
	// * https://github.com/census-instrumentation/opencensus-go/issues/865
	// * https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	nextSpanID uint64
	spanIDInc  uint64

	traceIDAdd  [2]uint64
	traceIDRand *rand.Rand
}

// NewSpanID returns a non-zero span ID from a randomly-chosen sequence.
func (gen *defaultIDGenerator) NewSpanID() [8]byte {
	var id uint64
	for id == 0 {
		id = atomic.AddUint64(&gen.nextSpanID, gen.spanIDInc)
	}
	var sid [8]byte
	binary.LittleEndian.PutUint64(sid[:], id)
	return sid
}

// NewTraceID returns a non-zero trace ID from a randomly-chosen sequence.
// mu should be held while this function is called.
func (gen *defaultIDGenerator) NewTraceID() [16]byte {
	var tid [16]byte
	// Construct the trace ID from two outputs of traceIDRand, with a constant
	// added to each half for additional entropy.
	gen.Lock()
	binary.LittleEndian.PutUint64(tid[0:8], gen.traceIDRand.Uint64()+gen.traceIDAdd[0])
	binary.LittleEndian.PutUint64(tid[8:16], gen.traceIDRand.Uint64()+gen.traceIDAdd[1])
	gen.Unlock()
	return tid
}
