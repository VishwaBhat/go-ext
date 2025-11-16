package slices2

func Filter[T any](elements []T, filter func(T) bool) []T {
	filtered := make([]T, 0, len(elements))
	for _, el := range elements {
		if filter(el) {
			continue
		}
		filtered = append(filtered, el)
	}
	return filtered
}

func Map[T, U any](elements []T, mapFunc func(T) U) []U {
	mapped := make([]U, 0, len(elements))
	for _, el := range elements {
		mapped = append(mapped, mapFunc(el))
	}
	return mapped
}

func MapWithErr[T, U any](elements []T, mapFunc func(T) (U, error)) ([]U, error) {
	mapped := make([]U, 0, len(elements))
	for _, el := range elements {
		val, err := mapFunc(el)
		if err != nil {
			return nil, err
		}
		mapped = append(mapped, val)
	}
	return mapped, nil
}

func Count[T any](elements []T, countFunc func(T) bool) int {
	count := 0
	for _, el := range elements {
		if countFunc(el) {
			count++
		}
	}
	return count
}

type Summable interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 |
		~string
}

func Sum[T any, U Summable](elements []T, sumFunc func(T) U) U {
	var sum U
	for _, el := range elements {
		sum += sumFunc(el)
	}
	return sum
}

func Unique[T comparable](elements []T, equal func(t1, t2 T) bool) []T {
	duplicates := make(map[T]struct{})

	unique := make([]T, 0, len(elements))
	for i, el1 := range elements {
		if _, exists := duplicates[el1]; exists {
			continue
		}
		for j, el2 := range elements {
			if i == j {
				continue
			}
			if equal(el1, el2) {
				duplicates[el2] = struct{}{}
			}
		}
		unique = append(unique, el1)
	}

	return unique
}

func Each[T comparable](elements []T, fn func(T)) {
	for _, el := range elements {
		fn(el)
	}
}
