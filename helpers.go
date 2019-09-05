package main

// OrderedStringSet ensures that duplicate string values can't be inserted and the order of those insertions is preserved
type OrderedStringSet struct {
	values   []string
	contains map[string]bool
}

// Contains return true if a string value already exists in OrderedStringSet
func (set OrderedStringSet) Contains(value string) bool {
	return set.contains[value]
}

// Add appends a value to the set if a duplicate string values does not already exists
func (set *OrderedStringSet) Add(value string) *OrderedStringSet {
	if _, ok := set.contains[value]; !ok {
		set.values = append(set.values, value)
		set.contains[value] = true
	}
	return set
}

// Len returns the current length of OrderedStringSet
func (set OrderedStringSet) Len() int {
	return len(set.values)
}

// Values returns a slice of values that were added to ordered set
func (set OrderedStringSet) Values() []string {
	return set.values
}

// NewOrderedStringSet returns a new instance of OrderedStringSet
func NewOrderedStringSet() OrderedStringSet {
	return OrderedStringSet{
		make([]string, 0),
		make(map[string]bool),
	}
}
