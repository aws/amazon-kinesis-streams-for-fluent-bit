package util

import (
	"math/rand"
	"time"
)

const (
	partitionKeyCharset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

type RandomStringGenerator struct {
	seededRandom *rand.Rand
	buffer       []byte
	Size         int
}

// Provides a generator of random strings of provided length
// it uses the math/rand library
func NewRandomStringGenerator(stringSize int) *RandomStringGenerator {

	return &RandomStringGenerator{
		seededRandom: rand.New(rand.NewSource(time.Now().UnixNano())),
		buffer:       make([]byte, stringSize),
		Size:         stringSize,
	}
}

func (gen *RandomStringGenerator) RandomString() string {
	for i := range gen.buffer {
		gen.buffer[i] = partitionKeyCharset[gen.seededRandom.Intn(len(partitionKeyCharset))]
	}
	return string(gen.buffer)
}
