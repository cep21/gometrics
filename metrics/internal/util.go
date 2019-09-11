package internal

// CopyOfMap does a deep copy of the map
func CopyOfMap(d map[string]string) map[string]string {
	if d == nil {
		return nil
	}
	ret := make(map[string]string, len(d))
	for k, v := range d {
		ret[k] = v
	}
	return ret
}
