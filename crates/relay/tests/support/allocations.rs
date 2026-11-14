//! Per-thread allocation budgets for synchronous native tests; other test threads are excluded.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Counting;

thread_local! {
    static BYTES: Cell<Option<usize>> = const { Cell::new(None) };
}

fn record(size: usize) {
    let _ = BYTES.try_with(|bytes| {
        if let Some(total) = bytes.get() {
            bytes.set(Some(total.saturating_add(size)));
        }
    });
}

// SAFETY: every allocation operation forwards its unchanged arguments to System.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

pub fn measure<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            BYTES.with(|bytes| bytes.set(None));
        }
    }
    BYTES.with(|bytes| {
        assert!(bytes.get().is_none(), "allocation measurements cannot nest");
        bytes.set(Some(0));
    });
    let reset = Reset;
    let result = operation();
    let allocated = BYTES.with(|bytes| bytes.get().unwrap());
    drop(reset);
    (result, allocated)
}
