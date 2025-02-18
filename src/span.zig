const std = @import("std");

const jdz_allocator = @import("jdz_allocator.zig");
const static_config = @import("static_config.zig");
const utils = @import("utils.zig");
const span_file = @import("span.zig");

const JdzAllocConfig = jdz_allocator.JdzAllocConfig;
const SizeClass = static_config.SizeClass;

const assert = std.debug.assert;

const span_size = static_config.span_size;
const span_header_size = static_config.span_header_size;
const page_size = static_config.page_size;
const mod_page_size = static_config.mod_page_size;
const free_list_null = static_config.free_list_null;

const invalid_pointer: usize = std.mem.alignBackward(usize, std.math.maxInt(usize), static_config.small_granularity);

pub fn Span(comptime thread_safe: bool) type {
    return extern struct {
        deferred_free_list: usize align(std.atomic.cache_line),

        free_list: usize,
        span_count: usize,
        alloc_ptr: usize,
        initial_ptr: usize,
        alloc_size: usize,

        class: SizeClass,

        arena: *anyopaque,
        next: ?*Self,
        prev: ?*Self,

        deferred_frees: u16,
        block_count: u16,

        full: bool,
        aligned_blocks: bool,

        const Self = @This();

        pub inline fn pushFreeList(self: *Self, buf: []u8) void {
            const ptr = @call(.always_inline, Self.getBlockPtr, .{ self, buf });

            @call(.always_inline, Self.pushFreeListElement, .{ self, ptr });

            self.block_count -= 1;
        }

        pub inline fn pushDeferredFreeList(self: *Self, buf: []u8) void {
            if (!thread_safe) unreachable;

            const ptr = @call(.always_inline, Self.getBlockPtr, .{ self, buf });

            @call(.always_inline, Self.pushDeferredFreeListElement, .{ self, ptr });
        }

        pub fn allocate(self: *Self) [*]u8 {
            if (self.free_list != free_list_null) {
                return self.popFreeListElement();
            }

            return self.allocateDeferredOrPtr();
        }

        pub fn allocateFromFreshSpan(self: *Self) [*]u8 {
            assert(self.isEmpty());

            return self.allocateFromAllocPtr();
        }

        pub fn allocateFromAllocPtr(self: *Self) [*]u8 {
            assert(self.alloc_ptr <= @intFromPtr(self) + span_size - self.class.block_size);

            self.block_count += 1;

            const next_page = self.alloc_ptr + page_size - (self.alloc_ptr & mod_page_size);
            const end_span = @intFromPtr(self) + span_size;
            const target = @min(end_span, next_page);
            const bytes_to_fill = target - self.alloc_ptr;
            const blocks_to_add = bytes_to_fill / self.class.block_size;

            const res: [*]u8 = @ptrFromInt(self.alloc_ptr);
            self.alloc_ptr += self.class.block_size;

            if (blocks_to_add > 1) {
                self.free_list = self.alloc_ptr;

                for (1..blocks_to_add) |_| {
                    self.pushFreeListElementForwardPointing();
                }

                @as(*usize, @ptrFromInt(self.alloc_ptr - self.class.block_size)).* = free_list_null;
            }

            return res;
        }

        pub fn allocateFromLargeSpan(self: *Self) [*]u8 {
            assert(self.isEmpty());

            self.block_count = 1;

            return @as([*]u8, @ptrFromInt(self.alloc_ptr));
        }

        pub inline fn popFreeListElement(self: *Self) [*]u8 {
            self.block_count += 1;

            const block = self.free_list;
            self.free_list = @as(*usize, @ptrFromInt(block)).*;

            return @ptrFromInt(block);
        }

        pub fn initialiseFreshSpan(self: *Self, arena: *anyopaque, size_class: SizeClass) void {
            self.* = .{
                .arena = arena,
                .initial_ptr = self.initial_ptr,
                .alloc_ptr = @intFromPtr(self) + span_header_size,
                .alloc_size = self.alloc_size,
                .class = size_class,
                .free_list = free_list_null,
                .deferred_free_list = free_list_null,
                .full = false,
                .next = null,
                .prev = null,
                .block_count = 0,
                .deferred_frees = 0,
                .span_count = 1,
                .aligned_blocks = false,
            };
        }

        pub fn initialiseFreshLargeSpan(self: *Self, arena: *anyopaque, span_count: usize) void {
            assert(static_config.large_max <= std.math.maxInt(u32));

            self.* = .{
                .arena = arena,
                .initial_ptr = self.initial_ptr,
                .alloc_ptr = @intFromPtr(self) + span_header_size,
                .alloc_size = self.alloc_size,
                .class = .{
                    .block_size = @truncate(span_count * span_size - span_header_size),
                    .class_idx = undefined,
                    .block_max = 1,
                },
                .free_list = free_list_null,
                .deferred_free_list = free_list_null,
                .full = false,
                .next = null,
                .prev = null,
                .block_count = 0,
                .deferred_frees = 0,
                .span_count = span_count,
                .aligned_blocks = false,
            };
        }

        pub inline fn isFull(self: *Self) bool {
            return self.block_count == self.class.block_max and self.deferred_frees == 0;
        }

        pub inline fn isEmpty(self: *Self) bool {
            return self.block_count - self.deferred_frees == 0;
        }

        pub inline fn splitLastSpans(self: *Self, span_count: usize) *Self {
            return self.splitFirstSpansReturnRemaining(self.span_count - span_count);
        }

        pub inline fn splitFirstSpanReturnRemaining(self: *Self) *Self {
            return self.splitFirstSpansReturnRemaining(1);
        }

        pub fn splitFirstSpansReturnRemaining(self: *Self, span_count: usize) *Self {
            assert(self.span_count > span_count);

            const remaining_span_addr = @intFromPtr(self) + span_size * span_count;
            const remaining_span: *Self = @ptrFromInt(remaining_span_addr);
            remaining_span.span_count = self.span_count - span_count;
            remaining_span.alloc_size = self.alloc_size - (remaining_span_addr - self.initial_ptr);
            remaining_span.initial_ptr = remaining_span_addr;

            self.span_count = span_count;
            self.alloc_size = remaining_span.initial_ptr - self.initial_ptr;

            return remaining_span;
        }

        inline fn allocateDeferredOrPtr(self: *Self) [*]u8 {
            if (thread_safe and self.freeDeferredList()) {
                return self.popFreeListElement();
            }
            return self.allocateFromAllocPtr();
        }

        inline fn getBlockPtr(self: *Self, buf: []u8) [*]u8 {
            if (!self.aligned_blocks) {
                return buf.ptr;
            } else {
                const start_alloc_ptr = @intFromPtr(self) + span_header_size;
                const block_offset = @intFromPtr(buf.ptr) - start_alloc_ptr;

                return buf.ptr - block_offset % self.class.block_size;
            }
        }

        inline fn pushFreeListElementForwardPointing(self: *Self) void {
            const next_block = self.alloc_ptr + self.class.block_size;
            @as(*usize, @ptrFromInt(self.alloc_ptr)).* = next_block;
            self.alloc_ptr = next_block;
        }

        inline fn pushFreeListElement(self: *Self, ptr: [*]u8) void {
            const block: *?usize = @ptrCast(@alignCast(ptr));
            block.* = self.free_list;
            self.free_list = @intFromPtr(block);
        }

        inline fn pushDeferredFreeListElement(self: *Self, ptr: [*]u8) void {
            const block: *usize = @ptrCast(@alignCast(ptr));

            while (true) {
                block.* = @atomicRmw(usize, &self.deferred_free_list, .Xchg, invalid_pointer, .acquire);

                if (block.* != invalid_pointer) {
                    break;
                }
            }

            self.deferred_frees += 1;

            @atomicStore(usize, &self.deferred_free_list, @intFromPtr(block), .release);
        }

        inline fn freeDeferredList(self: *Self) bool {
            assert(self.free_list == free_list_null);

            if (self.deferred_free_list == free_list_null) return false;

            while (true) {
                self.free_list = @atomicRmw(usize, &self.deferred_free_list, .Xchg, invalid_pointer, .acquire);

                if (self.free_list != invalid_pointer) {
                    break;
                }
            }
            self.block_count -= self.deferred_frees;
            self.deferred_frees = 0;

            @atomicStore(usize, &self.deferred_free_list, free_list_null, .release);

            return true;
        }
    };
}
