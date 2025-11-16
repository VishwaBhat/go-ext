package set

type Set[T comparable] struct {
	data map[T]struct{}
}

func NewSet[T comparable]() *Set[T] {
	return &Set[T]{
		data: make(map[T]struct{}),
	}
}

func (s *Set[T]) Add(el T) bool {
	if _, ok := s.data[el]; ok {
		return ok
	}
	s.data[el] = struct{}{}
	return false
}

func (s *Set[T]) Remove(el T) bool {
	if _, ok := s.data[el]; !ok {
		return ok
	}
	delete(s.data, el)
	return true
}

func (s *Set[T]) Contains(el T) bool {
	_, ok := s.data[el]
	return ok
}

func (s *Set[T]) Cardinality() int {
	return len(s.data)
}

func (s *Set[T]) ToSlice() []T {
	sl := make([]T, 0, len(s.data))
	for val := range s.data {
		sl = append(sl, val)
	}
	return sl
}
