const std = @import("std");
const jdz_allocator = @import("jdz_allocator.zig");
const utils = @import("utils.zig");
const span_file = @import("span.zig");

const JdzAllocConfig = jdz_allocator.JdzAllocConfig;
const testing = std.testing;
const assert = utils.assert;


pub fn DeferredSpanList(comptime thread_safe: bool) type {
    const Span = span_file.Span(thread_safe);
    return struct {
        head: ?*Span = null,

        const Self = @This();

        pub fn write(self: *Self, span: *Span) void {
            while (true) {
                span.next = self.head;

                if (@cmpxchgWeak(?*Span, &self.head, span.next, span, .monotonic, .monotonic) == null) {
                    return;
                }
            }
        }

        pub fn getAndRemoveList(self: *Self) ?*Span {
            return @atomicRmw(?*Span, &self.head, .Xchg, null, .monotonic);
        }
    };
}
