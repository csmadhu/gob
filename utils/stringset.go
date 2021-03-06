package utils

import "sort"

// StringSet implements Set interface
type StringSet map[string]struct{}

// NewStringSet returns set initialized with items
func NewStringSet(items ...string) StringSet {
	set := make(StringSet)
	for _, item := range items {
		set[item] = struct{}{}
	}

	return set
}

// Contains return true if set contains item
func (set StringSet) Contains(item string) bool {
	_, ok := set[item]
	return ok
}

// Insert adds item to set
func (set StringSet) Insert(item string) {
	set[item] = struct{}{}
}

// Len returns number of items in set
func (set StringSet) Len() int {
	return len(set)
}

// Remove an item from set
func (set StringSet) Remove(item string) {
	delete(set, item)
}

// Intersetion returns common elements of set and other
func (set StringSet) Intersetion(other StringSet) StringSet {
	result := NewStringSet()
	for item := range set {
		if other.Contains(item) {
			result.Insert(item)
		}
	}

	return result
}

// Difference returns elements present in set and not present in other
func (set StringSet) Difference(other StringSet) StringSet {
	result := NewStringSet()
	for item := range set {
		if !other.Contains(item) {
			result.Insert(item)
		}
	}

	return result
}

// Union returns all elements of set and other
func (set StringSet) Union(other StringSet) StringSet {
	result := NewStringSet()
	for item := range set {
		result.Insert(item)
	}

	for item := range other {
		result.Insert(item)
	}

	return result
}

// Equals returns false if other StringSet does not contain exact items
func (set StringSet) Equals(other StringSet) bool {
	if set.Len() != other.Len() {
		return false
	}

	for item := range set {
		if !other.Contains(item) {
			return false
		}
	}

	return true
}

// ToSlice return set of items in sorted order
func (set StringSet) ToSlice() (items []string) {
	for item := range set {
		items = append(items, item)
	}

	sort.Strings(items)
	return items
}
