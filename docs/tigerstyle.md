# TigerStyle for Rust

Coding conventions for this codebase, adapted from [TigerStyle](https://tigerstyle.dev/).
These patterns prioritize correctness and debuggability over brevity.

## Type System

**Use newtypes to prevent mixing up values:**

```rust
struct UserId(u64);
struct OrderId(u64);
fn process_order(user: UserId, order: OrderId) { }  // Can't swap arguments
```

**Encode states in types so invalid states won't compile:**

```rust
// Avoid boolean flags that allow nonsense combinations
enum Connection {
    Disconnected,
    Connected(Socket),
    Authenticated { socket: Socket, session: Session },
}
```

**Parse into validated types instead of validating repeatedly:**

```rust
struct Email(String);

impl Email {
    fn parse(s: &str) -> Result<Self, Error> {
        if !is_valid_email(s) { return Err(Error::InvalidEmail); }
        Ok(Self(s.to_owned()))
    }
}

fn send_email(email: &Email) { }  // Only accepts validated emails
```

## Error Handling

Every `Result` must be handled. Avoid `unwrap()` in non-test code.

```rust
// Propagate with context
file.write_all(data)
    .map_err(|e| Error::WriteFailed { path: path.clone(), source: e })?;
```

`unwrap()` is acceptable:
- In tests
- After a check that guarantees success (with a comment)
- In initialization that should panic on misconfiguration

```rust
let first = items.first().expect("items is non-empty: checked at line 42");
```

Define explicit error types per module:

```rust
mod superblock {
    #[derive(Debug)]
    pub enum Error {
        Fork,
        NotFound,
        QuorumLost,
    }
}
```

## Assertions

Use assertions to document and enforce invariants. Aim for 2+ per non-trivial
function, especially at boundaries.

```rust
fn commit(&mut self, op: u64, prepare: &Message) {
    assert!(self.status == Status::Normal || self.status == Status::ViewChange);
    assert!(op == self.commit_min + 1);
    assert!(prepare.header.op == op);

    // ... commit logic ...

    assert!(self.commit_min == op);  // postcondition
}
```

Split compound assertions for clear failure messages:

```rust
// Bad: which condition failed?
assert!(a && b && c);

// Good
assert!(a);
assert!(b);
assert!(c);
```

Compile-time assertions for layout guarantees:

```rust
const _: () = {
    assert!(std::mem::size_of::<Account>() == 128);
    assert!(std::mem::align_of::<Account>() == 16);
};
```

## Bounded Resources

All resources must have explicit bounds. No unbounded growth.

```rust
const CONNECTIONS_MAX: u32 = 64;
const TIMEOUT_MAX: Duration = Duration::from_secs(60);

struct BoundedQueue<T> {
    items: Vec<T>,
    capacity: usize,
}

impl<T> BoundedQueue<T> {
    fn push(&mut self, item: T) -> Result<(), QueueFull> {
        if self.items.len() >= self.capacity {
            return Err(QueueFull);
        }
        self.items.push(item);
        Ok(())
    }
}
```

## Memory

**Avoid allocation in hot paths.** Pre-allocate and reuse buffers:

```rust
struct RequestProcessor {
    buffer: Vec<u8>,
}

impl RequestProcessor {
    fn new() -> Self {
        Self { buffer: Vec::with_capacity(MAX_REQUEST_SIZE) }
    }

    fn process(&mut self, req: &Request) -> Response {
        self.buffer.clear();  // reuse, no allocation
        // ...
    }
}
```

**Minimize cloning.** Cloning often indicates unclear ownership - restructure
to use references when possible.

**Pass large values by reference.** Anything over 16 bytes should be `&T`.

## Wire Format Types

Use `#[repr(C)]` for types that cross boundaries:

```rust
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Account {
    pub id: u128,
    pub debits_pending: u128,
    pub ledger: u32,
    pub flags: AccountFlags,
}

const _: () = { assert!(std::mem::size_of::<Account>() == 48); };
```

Use explicit integer sizes (`u64`, `i32`) instead of `usize`/`isize` for
serialized data.

## State Machines

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Normal,
    ViewChange,
    Recovering,
}

impl Replica {
    fn transition_to_view_change(&mut self, view_new: u32) {
        assert!(matches!(self.status, Status::Normal | Status::ViewChange));
        assert!(view_new >= self.view);

        self.status = Status::ViewChange;
        self.view = view_new;
    }
}
```

## Naming

Be precise and include units:

```rust
let latency_ms_max = 1000;      // units at end
let buffer_size_bytes = 4096;
let item_count = items.len();   // not cnt or num_items
```

Avoid abbreviations unless universal (`id`, `url`, `api`).

## Function Length

Keep functions under 70 lines. Extract logical sub-operations:

```rust
fn commit(&mut self) {
    self.commit_prefetch();
}

fn commit_prefetch(&mut self) {
    // ...
    self.commit_execute();
}

fn commit_execute(&mut self) {
    // ...
}
```

## Avoid Recursion

Recursion risks stack overflow. Convert to iteration with explicit bounds:

```rust
fn process_tree(root: &Node) {
    let mut stack = Vec::with_capacity(MAX_DEPTH);
    stack.push(root);

    while let Some(node) = stack.pop() {
        assert!(stack.len() < MAX_DEPTH, "tree depth exceeds maximum");
        stack.extend(node.children());
    }
}
```

## Dependencies

Before adding a dependency:
1. Can we implement this in < 100 lines?
2. Is it actively maintained?
3. What's the transitive dependency tree?
4. Do we need the whole crate or just one function?
