# Go helper utilities

This repository contains a collection of helper utilities for G, aiming to simplify common tasks and 
improve code readability. I use this mostly for personal use, feel free to use it at your expense. In case you
find any other utility that could help others, feel free to raise a PR against this repo. 

## Packages

### `slices2` - Slice Utilities

The `slices` package offers a collection of generic utility functions for working with slices, including filtering,
mapping, counting, and summing. I intentionally named it `slices2` to avoid incorrect imports with the built-in `slices` 
package.

- `Filter`: Filters slices based on the given condition and returns new one
  ```go
  evenNums := slices2.Filter(numbers, func(v int) bool { return v % 2 ==0 })
  ```

- `Map`: Transforms given elements to a different type
  ```go
  strNums := slices2.Map(numbers, func(v int) string { return fmt.Sprint(v) })
  ```

- `Count`: Counts no of elements matching the given criteria
  ```go
  totalEvenNums := slices2.Count(numbers, func(v int) bool { return v % 2 == 0 })
  ```

- `Sum`: Adds results across each element's computation to provide a cumulative sum
  ```go
  sumOfSquares := slices.Sum(numbers, func(n int) int { return n * n})
  ```

- `Each`: Iterates through each element to execute function
  ```go
  slices2.Each(numbers, func(n int) { 
    fmt.Printf("%d ", n) 
  })
  ```

- `Unique`: Returns unique elements in the given slice
  ```go
    type Person struct {
        ID   int
        Name string
    }
  
    people := []Person{
        {ID: 1, Name: "Alice"},
        {ID: 2, Name: "Bob"},
        {ID: 1, Name: "Alice"}, // Duplicate
        {ID: 3, Name: "Charlie"},
    }
    
    uniq := slices.Unique(numbers, 
      func (p1, p2 Person) bool { 
        return p1.ID == p2.ID
      },
    )
  ```


### `set` - Generic Set Implementation

The `set` package provides a generic set data structure for any comparable type.

**Usage:**

```go
func main() {
    s := set.NewSet[string]()
    
    s.Add("apple")
    s.Add("banana")
    s.Add("apple") // Adding duplicate has no effect
    
    fmt.Println(s.Contains("apple")) // true
    fmt.Println(s.Contains("orange")) // false
    fmt.Println(s.Cardinality()) // 2
    
    s.Remove("banana")
    fmt.Printf(s.Contains("banana")) // false
    fmt.Printf(s.Cardinality()) // 1
    
    slice := s.ToSlice()
    fmt.Printf(slice) // [apple] (order not guaranteed)
}
```

### `conc` - Concurrency Utilities

The `conc` package provides tools for managing concurrent operations,
making it easier to work with goroutines and handle parallel tasks.

#### `Tasker`

A simple goroutine pool that limits the number of concurrent tasks. It allows enqueuing tasks and waiting for their
completion, returning results and the first error encountered.

**Usage:**

```go
func main() {
    // Create a new Tasker with a limit of 2 concurrent goroutines
    // and a timeout of 5 seconds for all tasks.
    tasker := conc.NewTasker[string, int](
      conc.WithMaxGoRoutines(2),
      conc.WithTimeout(5*time.Second),
    )
    
    // Set the task function. This function will be executed for each enqueued item.
    tasker.SetTask(func (v string) (int, error) {
      if len(v) <= 3 {
        return 0, fmt.Errorf("input(%s) cannot be less than 3 characters", v)
      }
      // Simulate an expensive computation
      time.Sleep(100 * time.Millisecond)
      return len(v), nil
    })
    
    // Enqueue tasks as on when the data is ready
    tasker.Enqueue("hello")
    tasker.Enqueue("world")
    tasker.Enqueue("programming")
    
    // Wait for all tasks to complete and get the results
    vals, err := tasker.Wait()
    fmt.Println(vals, err)
}
```

#### `Runner`

A utility for concurrent mapping and pipelining of slices with configurable concurrency limits and timeouts. It supports
`Map`, `MapWithContext`, and `Pipeline` operations.

**Usage:**

```go

// --- Map Example ---
runner := conc.NewRunner[int, string](
conc.WithMaxGoRoutines(2),
conc.WithTimeout(5*time.Second),
)
numbers := []int{1, 2, 3, 4, 5}

// Define a mapping function
mapFn := func (n int) (string, error) {
  time.Sleep(100 * time.Millisecond) // Simulate work
  if n == 3 {
    return "", fmt.Errorf("error processing number 3")
  }
  return fmt.Sprintf("Number: %d", n), nil
}

// Map the numbers concurrently
results, err := runner.Map(numbers, mapFn)
if err != nil {
  fmt.Printf("Map error: %v\n", err) // Map error: error processing number 3
} else {
  fmt.Printf("Map results: %v\n", results)
}
}
```

### Disclaimer

I know utilities may clash with Go's philosophy of explicitness, and the community often resists abstractions that hide 
complexity. Still, I find some patterns, especially slice operations and concurrent constructs, make the code verbose 
enough that they hurt readability.