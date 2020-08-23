package utils

import (
	"reflect"
	"testing"
)

func TestStringSet(t *testing.T) {
	set1 := NewStringSet("a", "b", "c")
	set2 := NewStringSet("c", "d", "e")

	t.Run("len", func(t *testing.T) {
		if set1.Len() != 3 {
			t.Fatalf("len got: %d want: %d", set1.Len(), 3)
		}
	})

	t.Run("contains", func(t *testing.T) {
		if set1.Contains("d") {
			t.Fatalf("set does not contain item d")
		}
	})

	t.Run("insert", func(t *testing.T) {
		set2.Insert("f")
		if !set2.Contains("f") {
			t.Fatalf("set does not contain item f")
		}
	})

	t.Run("intersection", func(t *testing.T) {
		i := set1.Intersetion(set2)
		if i.Len() != 1 {
			t.Fatalf("set1 and set2 contains one item in common")
		}

		want := []string{"c"}
		if !reflect.DeepEqual(i.ToSlice(), want) {
			t.Fatalf("intersection of two sets got: %v want: %v", i.ToSlice(), want)
		}
	})

	t.Run("difference", func(t *testing.T) {
		d := set1.Difference(set2)
		if d.Len() != 2 {
			t.Fatalf("set1 contains 2 items not present in set2")
		}

		want := []string{"a", "b"}
		if !reflect.DeepEqual(d.ToSlice(), want) {
			t.Fatalf("difference of two sets got: %v want: %v", d.ToSlice(), want)
		}
	})

	t.Run("union", func(t *testing.T) {
		u := set1.Union(set2)
		if u.Len() != 6 {
			t.Fatalf("union of two sets should return 5 items")
		}

		want := []string{"a", "b", "c", "d", "e", "f"}
		if !reflect.DeepEqual(u.ToSlice(), want) {
			t.Fatalf("union of two sets got: %v want: %v", u.ToSlice(), want)
		}
	})

	t.Run("remove", func(t *testing.T) {
		set1.Remove("c")
		if set1.Contains("c") {
			t.Fatalf("failed to remove item c from set")
		}
	})
}
