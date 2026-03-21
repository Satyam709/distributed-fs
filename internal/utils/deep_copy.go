package utils

func DeepCopy[T any](registry map[string]*T) map[string]T {
	nmap := make(map[string]T, len(registry))
	var zero T
	for k, v := range registry {
		if v == nil {
			nmap[k] = zero
			continue
		}
		nmap[k] = *v
	}
	return nmap
}
