package redis

//* Hash

// Type to hold Redis hash return values.
type Hash map[string]Value

// NewHash creates a new empty hash.
func NewHash() Hash {
	return make(Hash)
}

// String returns the value of a key as string.
func (h Hash) String(k string) string {
	if v, ok := h[k]; ok {
		return v.String()
	}

	return ""
}

// Bool returns the value of a key as bool.
func (h Hash) Bool(k string) bool {
	if v, ok := h[k]; ok {
		return v.Bool()
	}

	return false
}

// Int returns the value of a key as int.
func (h Hash) Int(k string) int {
	if v, ok := h[k]; ok {
		return v.Int()
	}

	return 0
}

// Int64 returns the value of a key as int64.
func (h Hash) Int64(k string) int64 {
	if v, ok := h[k]; ok {
		return v.Int64()
	}

	return 0
}

// Uint64 returns the value of a key as uint64.
func (h Hash) Uint64(k string) uint64 {
	if v, ok := h[k]; ok {
		return v.Uint64()
	}

	return 0
}

// Float64 returns the value of a key as float64.
func (h Hash) Float64(k string) float64 {
	if v, ok := h[k]; ok {
		return v.Float64()
	}

	return 0.0
}

// Bytes returns the value of a key as byte slice.
func (h Hash) Bytes(k string) []byte {
	if v, ok := h[k]; ok {
		return v.Bytes()
	}

	return []byte{}
}

// StringSlice returns the value of a key as string slice.
func (h Hash) StringSlice(k string) []string {
	if v, ok := h[k]; ok {
		return v.StringSlice()
	}

	return []string{}
}

// StringMap returns the value of a key as string map.
func (h Hash) StringMap(k string) map[string]string {
	if v, ok := h[k]; ok {
		return v.StringMap()
	}

	return map[string]string{}
}
