const std = @import("std");
const builtin = @import("builtin");

const jdz = @import("jdz_allocator.zig");
const shared_arena_handler = @import("shared_arena_handler.zig");
const span_arena = @import("arena.zig");
const static_config = @import("static_config.zig");
const utils = @import("utils.zig");
const span_file = @import("span.zig");

const Span = span_file.Span;
const JdzAllocConfig = jdz.JdzAllocConfig;
const Value = std.atomic.Value;

const log2 = std.math.log2;
const testing = std.testing;
const assert = std.debug.assert;

pub fn JdzAllocator(comptime config: JdzAllocConfig) type {
    const Arena = span_arena.Arena(config, false);

    const SharedArenaHandler = shared_arena_handler.SharedArenaHandler(config);

    assert(config.span_alloc_count >= 1);

    // currently not supporting page sizes greater than 64KiB
    assert(page_size <= span_size);

    // -1 as one span gets allocated to span list and not cache
    assert(config.span_alloc_count - 1 <= config.cache_limit);

    assert(span_header_size >= @sizeOf(Span));
    assert(config.large_span_overhead_mul >= 0.0);

    // These asserts must be true for alignment purposes
    assert(utils.isPowerOfTwo(span_header_size));
    assert(utils.isPowerOfTwo(small_granularity));
    assert(utils.isPowerOfTwo(small_max));
    assert(utils.isPowerOfTwo(medium_granularity));
    assert(medium_granularity <= small_max);
    assert(span_header_size % small_granularity == 0);

    // These asserts must be true for MPSC queue to work
    assert(config.large_cache_limit > 1);
    assert(utils.isPowerOfTwo(config.large_cache_limit));

    return struct {
        backing_allocator: std.mem.Allocator,
        arena_handler: SharedArenaHandler,

        const Self = @This();

        pub fn init() Self {
            return .{
                .backing_allocator = config.backing_allocator,
                .arena_handler = SharedArenaHandler.init(),
            };
        }

        pub fn deinit(self: *Self) void {
            const spans_leaked = self.arena_handler.deinit();

            if (config.report_leaks) {
                const log = std.log.scoped(.jdz_allocator);

                if (spans_leaked != 0) {
                    log.warn("{} leaked 64KiB spans", .{spans_leaked});
                }
            }
        }

        pub fn allocator(self: *Self) std.mem.Allocator {
            return .{
                .ptr = self,
                .vtable = &.{
                    .alloc = alloc,
                    .resize = resize,
                    .remap = remap,
                    .free = free,
                },
            };
        }

        fn alloc(ctx: *anyopaque, len: usize, log2_align: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            _ = ret_addr;

            const self: *Self = @ptrCast(@alignCast(ctx));

            const log2_align_value = @intFromEnum(log2_align);
            if (log2_align_value <= small_granularity_shift) {
                return @call(.always_inline, allocate, .{ self, len });
            }

            const alignment = @as(usize, 1) << @intCast(log2_align_value);
            const size = @max(alignment, len);

            if (size <= span_header_size) {
                const aligned_block_size: usize = @call(.always_inline, utils.roundUpToPowerOfTwo, .{size});

                assert(aligned_block_size >= size);

                return @call(.always_inline, allocate, .{ self, aligned_block_size });
            }

            assert(alignment < span_effective_size);

            if (@call(.always_inline, allocate, .{ self, size + alignment })) |block_ptr| {
                const align_mask: usize = alignment - 1;
                var ptr = block_ptr;

                if (@intFromPtr(ptr) & align_mask != 0) {
                    ptr = @ptrFromInt((@intFromPtr(ptr) & ~align_mask) + alignment);
                    const span = @call(.always_inline, utils.getSpan, .{ptr});

                    span.aligned_blocks = true;
                }

                return ptr;
            }

            return null;
        }

        fn resize(ctx: *anyopaque, buf: []u8, log2_align: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
            _ = ret_addr;
            _ = ctx;
            const alignment = @as(usize, 1) << @intCast(@intFromEnum(log2_align));
            const aligned = (@intFromPtr(buf.ptr) & (alignment - 1)) == 0;

            const span = @call(.always_inline, utils.getSpan, .{buf.ptr});

            if (buf.len <= span_max) return new_len <= span.class.block_size and aligned;
            if (buf.len <= large_max) return new_len <= span.alloc_size - (span.alloc_ptr - span.initial_ptr) and aligned;

            // round up to greater than or equal page size
            const max_len = (buf.len - 1 / page_size) * page_size + page_size;

            return aligned and new_len <= max_len;
        }

        fn remap(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, new_len: usize, return_address: usize) ?[*]u8 {
            if (resize(ctx, buf, alignment, new_len, return_address)) {
                return buf.ptr;
            }
            return null;
        }


        fn free(ctx: *anyopaque, buf: []u8, log2_align: std.mem.Alignment, ret_addr: usize) void {
            _ = ctx;
            _ = ret_addr;
            _ = log2_align;

            const span = @call(.always_inline, utils.getSpan, .{buf.ptr});

            const arena: *Arena = @ptrCast(@alignCast(span.arena));

            if (span.class.block_size <= medium_max) {
                @call(.always_inline, Arena.freeSmallOrMedium, .{ arena, span, buf });
            } else if (span.class.block_size <= large_max) {
                @call(.always_inline, Arena.cacheLargeSpanOrFree, .{ arena, span });
            } else {
                @call(.always_inline, Arena.freeHuge, .{ arena, span });
            }
        }

        fn allocate(self: *Self, size: usize) ?[*]u8 {
            const arena = @call(.always_inline, SharedArenaHandler.getArena, .{&self.arena_handler}) orelse return null;
            defer @call(.always_inline, Arena.release, .{arena});

            if (size <= small_max) {
                const size_class = @call(.always_inline, utils.getSmallSizeClass, .{size});
                return @call(.always_inline, Arena.allocateToSpan, .{ arena, size_class });
            } else if (size <= medium_max) {
                const size_class = @call(.always_inline, utils.getMediumSizeClass, .{size});
                return @call(.always_inline, Arena.allocateToSpan, .{ arena, size_class });
            } else if (size <= span_max) {
                return @call(.always_inline, Arena.allocateOneSpan, .{ arena, span_class });
            } else if (size <= large_max) {
                const span_count = @call(.always_inline, utils.getSpanCount, .{size});
                return @call(.always_inline, Arena.allocateToLargeSpan, .{ arena, span_count });
            } else {
                const span_count = @call(.always_inline, utils.getHugeSpanCount, .{size}) orelse return null;
                return @call(.always_inline, Arena.allocateHuge, .{ arena, span_count });
            }
        }

        pub fn usableSize(self: *Self, ptr: *anyopaque) usize {
            _ = self;

            const span = utils.getSpan(ptr);

            if (span.span_count == 1) {
                return span.class.block_size;
            }

            return span.alloc_size - (span.alloc_ptr - span.initial_ptr);
        }
    };
}

const SizeClass = static_config.SizeClass;

const span_size = static_config.span_size;
const span_effective_size = static_config.span_effective_size;
const span_header_size = static_config.span_header_size;
const span_upper_mask = static_config.span_upper_mask;

const small_granularity = static_config.small_granularity;
const small_granularity_shift = static_config.small_granularity_shift;
const small_max = static_config.small_max;

const medium_granularity = static_config.medium_granularity;
const medium_granularity_shift = static_config.medium_granularity_shift;
const medium_max = static_config.medium_max;

const span_max = static_config.span_max;
const span_class = static_config.span_class;

const large_max = static_config.large_max;

const page_size = static_config.page_size;
const page_alignment = static_config.page_alignment;

const small_size_classes = static_config.small_size_classes;
const medium_size_classes = static_config.medium_size_classes;

const aligned_size_classes = static_config.aligned_size_classes;
const aligned_spans_offset = static_config.aligned_spans_offset;
const span_align_max = static_config.span_align_max;

//
// Tests
//
test "small allocations - free in same order" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    var list = std.ArrayList(*u64).init(std.testing.allocator);
    defer list.deinit();

    var i: usize = 0;
    while (i < 513) : (i += 1) {
        const ptr = try allocator.create(u64);
        try list.append(ptr);
    }

    for (list.items) |ptr| {
        allocator.destroy(ptr);
    }
}

test "small allocations - free in reverse order" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    var list = std.ArrayList(*u64).init(std.testing.allocator);
    defer list.deinit();

    var i: usize = 0;
    while (i < 513) : (i += 1) {
        const ptr = try allocator.create(u64);
        try list.append(ptr);
    }

    while (list.pop()) |ptr| {
        allocator.destroy(ptr);
    }
}

test "small allocations - alloc free alloc" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const a = try allocator.create(u64);
    allocator.destroy(a);
    const b = try allocator.create(u64);
    allocator.destroy(b);
}

test "large allocations" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const ptr1 = try allocator.alloc(u64, 42768);
    const ptr2 = try allocator.alloc(u64, 52768);
    allocator.free(ptr1);
    const ptr3 = try allocator.alloc(u64, 62768);
    allocator.free(ptr3);
    allocator.free(ptr2);
}

test "very large allocation" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    try std.testing.expectError(error.OutOfMemory, allocator.alloc(u8, std.math.maxInt(usize)));
}

test "realloc" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    var slice = try allocator.alignedAlloc(u8, @alignOf(u32), 1);
    defer allocator.free(slice);
    slice[0] = 0x12;

    // This reallocation should keep its pointer address.
    const old_slice = slice;
    slice = try allocator.realloc(slice, 2);
    try std.testing.expect(old_slice.ptr == slice.ptr);
    try std.testing.expect(slice[0] == 0x12);
    slice[1] = 0x34;

    // This requires upgrading to a larger bin size
    slice = try allocator.realloc(slice, 17);
    try std.testing.expect(old_slice.ptr != slice.ptr);
    try std.testing.expect(slice[0] == 0x12);
    try std.testing.expect(slice[1] == 0x34);
}

test "shrink" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    var slice = try allocator.alloc(u8, 20);
    defer allocator.free(slice);

    @memset(slice, 0x11);

    try std.testing.expect(allocator.resize(slice, 17));
    slice = slice[0..17];

    for (slice) |b| {
        try std.testing.expect(b == 0x11);
    }

    try std.testing.expect(allocator.resize(slice, 16));

    for (slice) |b| {
        try std.testing.expect(b == 0x11);
    }
}

test "large object - grow" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    var slice1 = try allocator.alloc(u8, 8192 - 10);
    defer allocator.free(slice1);

    const old = slice1;
    slice1 = try allocator.realloc(slice1, 8192 - 10);
    try std.testing.expect(slice1.ptr == old.ptr);

    slice1 = try allocator.realloc(slice1, 8192);
    try std.testing.expect(slice1.ptr == old.ptr);

    slice1 = try allocator.realloc(slice1, 8192 + 1);
}

test "realloc small object to large object" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    var slice = try allocator.alloc(u8, 70);
    defer allocator.free(slice);
    slice[0] = 0x12;
    slice[60] = 0x34;

    // This requires upgrading to a large object
    const large_object_size = 8192 + 50;
    slice = try allocator.realloc(slice, large_object_size);
    try std.testing.expect(slice[0] == 0x12);
    try std.testing.expect(slice[60] == 0x34);
}

test "shrink large object to large object" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    var slice = try allocator.alloc(u8, 8192 + 50);
    defer allocator.free(slice);
    slice[0] = 0x12;
    slice[60] = 0x34;

    if (!allocator.resize(slice, 8192 + 1)) return;
    slice = slice.ptr[0 .. 8192 + 1];
    try std.testing.expect(slice[0] == 0x12);
    try std.testing.expect(slice[60] == 0x34);

    try std.testing.expect(allocator.resize(slice, 8192 + 1));
    slice = slice[0 .. 8192 + 1];
    try std.testing.expect(slice[0] == 0x12);
    try std.testing.expect(slice[60] == 0x34);

    slice = try allocator.realloc(slice, 8192);
    try std.testing.expect(slice[0] == 0x12);
    try std.testing.expect(slice[60] == 0x34);
}

test "shrink large object to large object with larger alignment" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    var debug_buffer: [1000]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&debug_buffer);
    const debug_allocator = fba.allocator();

    const alloc_size = 8192 + 50;
    var slice = try allocator.alignedAlloc(u8, 16, alloc_size);
    defer allocator.free(slice);

    const big_alignment: usize = switch (builtin.os.tag) {
        .windows => 65536, // Windows aligns to 64K.
        else => 8192,
    };
    // This loop allocates until we find a page that is not aligned to the big
    // alignment. Then we shrink the allocation after the loop, but increase the
    // alignment to the higher one, that we know will force it to realloc.
    var stuff_to_free = std.ArrayList([]align(16) u8).init(debug_allocator);
    while (std.mem.isAligned(@intFromPtr(slice.ptr), big_alignment)) {
        try stuff_to_free.append(slice);
        slice = try allocator.alignedAlloc(u8, 16, alloc_size);
    }
    while (stuff_to_free.pop()) |item| {
        allocator.free(item);
    }
    slice[0] = 0x12;
    slice[60] = 0x34;

    slice = try allocator.reallocAdvanced(slice, big_alignment, alloc_size / 2);
    try std.testing.expect(slice[0] == 0x12);
    try std.testing.expect(slice[60] == 0x34);
}

test "realloc large object to small object" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    var slice = try allocator.alloc(u8, 8192 + 50);
    defer allocator.free(slice);
    slice[0] = 0x12;
    slice[16] = 0x34;

    slice = try allocator.realloc(slice, 19);
    try std.testing.expect(slice[0] == 0x12);
    try std.testing.expect(slice[16] == 0x34);
}

test "realloc large object to larger alignment" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    var debug_buffer: [1000]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&debug_buffer);
    const debug_allocator = fba.allocator();

    var slice = try allocator.alignedAlloc(u8, 16, 8192 + 50);
    defer allocator.free(slice);

    const big_alignment: usize = switch (builtin.os.tag) {
        .windows => 65536, // Windows aligns to 64K.
        else => 8192,
    };
    // This loop allocates until we find a page that is not aligned to the big alignment.
    var stuff_to_free = std.ArrayList([]align(16) u8).init(debug_allocator);
    while (std.mem.isAligned(@intFromPtr(slice.ptr), big_alignment)) {
        try stuff_to_free.append(slice);
        slice = try allocator.alignedAlloc(u8, 16, 8192 + 50);
    }
    while (stuff_to_free.pop()) |item| {
        allocator.free(item);
    }
    slice[0] = 0x12;
    slice[16] = 0x34;

    slice = try allocator.reallocAdvanced(slice, 32, 8192 + 100);
    try std.testing.expect(slice[0] == 0x12);
    try std.testing.expect(slice[16] == 0x34);

    slice = try allocator.reallocAdvanced(slice, 32, 8192 + 25);
    try std.testing.expect(slice[0] == 0x12);
    try std.testing.expect(slice[16] == 0x34);

    slice = try allocator.reallocAdvanced(slice, big_alignment, 8192 + 100);
    try std.testing.expect(slice[0] == 0x12);
    try std.testing.expect(slice[16] == 0x34);
}

test "large object shrinks to small" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const slice = try allocator.alloc(u8, 8192 + 50);
    defer allocator.free(slice);

    try std.testing.expect(allocator.resize(slice, 4));
}

test "objects of size 1024 and 2048" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const slice = try allocator.alloc(u8, 1025);
    const slice2 = try allocator.alloc(u8, 3000);

    allocator.free(slice);
    allocator.free(slice2);
}

test "max large alloc" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const buf = try allocator.alloc(u8, large_max);
    defer allocator.free(buf);
}

test "huge alloc" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const buf = try allocator.alloc(u8, large_max + 1);
    defer allocator.free(buf);
}

test "huge alloc does not try to access span" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const buf1 = try allocator.alloc(u8, large_max + 1);
    const buf2 = try allocator.alloc(u8, large_max + 1);

    allocator.free(buf1);
    allocator.free(buf2);
}

test "small alignment small alloc" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const slice = allocator.rawAlloc(1, .@"4", @returnAddress()).?;
    defer allocator.rawFree(slice[0..1], .@"4", @returnAddress());

    try std.testing.expect(@intFromPtr(slice) % std.math.pow(u16, 2, 4) == 0);
}

test "medium alignment small alloc" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const alignment: u8 = @truncate(log2(utils.roundUpToPowerOfTwo(small_max + 1)));

    if (alignment > page_alignment) return error.SkipZigTest;

    const slice = allocator.rawAlloc(1, @enumFromInt(alignment), @returnAddress()).?;
    defer allocator.rawFree(slice[0..1], @enumFromInt(alignment), @returnAddress());

    try std.testing.expect(@intFromPtr(slice) % std.math.pow(u16, 2, 4) == 0);
}

test "page size alignment small alloc" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const slice = allocator.rawAlloc(1, @enumFromInt(page_alignment), @returnAddress()).?;
    defer allocator.rawFree(slice[0..1], @enumFromInt(page_alignment), @returnAddress());

    try std.testing.expect(@intFromPtr(slice) % std.math.pow(u16, 2, page_alignment) == 0);
}

test "small alignment large alloc" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const slice = allocator.rawAlloc(span_max, .@"4", @returnAddress()).?;
    defer allocator.rawFree(slice[0..span_max], .@"4", @returnAddress());

    try std.testing.expect(@intFromPtr(slice) % std.math.pow(u16, 2, 4) == 0);
}

test "medium alignment large alloc" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const alignment: u8 = @truncate(log2(utils.roundUpToPowerOfTwo(small_max + 1)));

    if (alignment > page_alignment) return error.SkipZigTest;

    const slice = allocator.rawAlloc(span_max, @enumFromInt(alignment), @returnAddress()).?;
    defer allocator.rawFree(slice[0..span_max], @enumFromInt(alignment), @returnAddress());

    try std.testing.expect(@intFromPtr(slice) % std.math.pow(u16, 2, 4) == 0);
}

test "page size alignment large alloc" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const slice = allocator.rawAlloc(span_max, @enumFromInt(page_alignment), @returnAddress()).?;
    defer allocator.rawFree(slice[0..span_max], @enumFromInt(page_alignment), @returnAddress());

    try std.testing.expect(@intFromPtr(slice) % std.math.pow(u16, 2, page_alignment) == 0);
}

test "consecutive overalignment" {
    var jdz_allocator = JdzAllocator(.{}).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const overaligned_sizes = [_]usize{ 192, 192, 192 };
    var buffers: [overaligned_sizes.len][]const u8 = undefined;

    for (overaligned_sizes, &buffers) |overaligned_size, *buffers_slot| {
        buffers_slot.* = try allocator.alignedAlloc(u8, 64, overaligned_size);
    }

    for (buffers) |buffer| {
        allocator.free(buffer);
    }
}

test "small allocations parallel" {
    var jdz_allocator = JdzAllocator(.{ .thread_safe = true }).init();
    defer jdz_allocator.deinit();

    const allocator = jdz_allocator.allocator();

    const spawn = struct {
        fn thread_spawn(alloc: std.mem.Allocator) !void {
            const a = try alloc.create(u64);
            alloc.destroy(a);
        }
    }.thread_spawn;

    var threads: [5]std.Thread = undefined;
    for (0..5) |i| {
        threads[i] = try std.Thread.spawn(.{}, spawn, .{allocator});
    }
    for (threads) |t| {
        t.join();
    }
}
