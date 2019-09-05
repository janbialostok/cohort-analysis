package main

import (
	"testing"

	"github.com/ZeFort/chance"
	"github.com/stretchr/testify/assert"
)

func TestOrderedStringSet(t *testing.T) {
	Chance := chance.New()
	set := NewOrderedStringSet()

	rand1 := Chance.String()
	set.Add(rand1)

	assert.Equal(t, 1, set.Len(), "should contain a single value after insertion")
	assert.True(t, set.Contains(rand1), "should contain the value that was added")

	set.Add(rand1)
	assert.Equal(t, 1, set.Len(), "should contain a single value after duplicate value was added")

	assert.Equal(t, 1, len(set.Values()), "should return a slice containing the added string value")
}
