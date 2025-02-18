const std = @import("std");
const span_arena = @import("arena.zig");
const jdz_allocator = @import("jdz_allocator.zig");
const utils = @import("utils.zig");

const JdzAllocConfig = jdz_allocator.JdzAllocConfig;

const assert = utils.assert;

var slots_counter: usize = 0;
pub const MAX_SLOTS = 256;

var thread_aid_counter: [MAX_SLOTS]u64 = @splat(0);

const ArenaDispatcher = packed struct {
    index: u32,
    capacity: u32,
};
const dispatcher_max_index = std.math.maxInt(u32)/2;

pub fn SharedArenaHandler(comptime config: JdzAllocConfig) type {
    // Verify batch size is power of two for optimization
    assert(utils.isPowerOfTwo(config.shared_arena_batch_size));
    const batch_size_mask = config.shared_arena_batch_size - 1;
    const Arena = span_arena.Arena(config, false);

    const Mutex = utils.getMutexType(config);

    return struct {
        const ArenasSet = struct {
            arenas: [config.shared_arena_batch_size]Arena,
            next: ?*ArenasSet,

            pub fn init() ArenasSet {
                var set: ArenasSet = .{
                    .arenas = undefined,
                    .next = null
                };

                for (&set.arenas) |*arena| {
                    arena.* = Arena.init();
                }

                return set;
            }
        };

        threadlocal var cached_thread_arenas: [MAX_SLOTS]?*Arena = @splat(null);

        first_arenas_set: if (config.thread_safe) ArenasSet else Arena,
        last_arenas_set: ?*ArenasSet = null,

        mutex: Mutex,
        handler_slot: usize,

        const Self = @This();


        pub fn init() Self {
            const slot = blk: {
                if (config.thread_safe) {
                    const _slot: usize = @atomicRmw(usize, &slots_counter, .Add, 1, .acquire);
                    if (_slot == MAX_SLOTS) {
                        // Maximum number of SharedArenaHandler instances has been reached.
                        // This implementation limits the total number of concurrent 
                        // JdzAllocator instances to MAX_SLOTS (256).
                        @panic("Maximum number of SharedArenaHandler instances exceeded");
                    }

                    thread_aid_counter[_slot] = @bitCast(ArenaDispatcher{.capacity = config.shared_arena_batch_size, .index = 0});
                    break :blk _slot;
                }
                break :blk 0;
            };

            return .{
                .first_arenas_set = blk: {
                    if (config.thread_safe) {
                        break :blk ArenasSet.init();
                    }
                    break :blk Arena.init();
                },
                .mutex = .{},
                .handler_slot = slot,
            };
        }

        pub fn deinit(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();

            const first_arenas_set = &self.first_arenas_set;

            if (!config.thread_safe) {
                return first_arenas_set.deinit();
            }

            var spans_leaked: usize = 0;
            var opt_arenas_set: ?*ArenasSet = first_arenas_set;

            while (opt_arenas_set) |arenas_set| {
                const next = arenas_set.next;

                for (&arenas_set.arenas) |*arena| {
                    spans_leaked += arena.deinit();
                }

                if (first_arenas_set != opt_arenas_set) {
                    config.backing_allocator.destroy(arenas_set);
                }
                opt_arenas_set = next;
            }

            @memset(&first_arenas_set.arenas, undefined);
            first_arenas_set.next = null;
            return spans_leaked;
        }

        pub fn getArena(self: *Self) ?*Arena {
            if (!config.thread_safe) {
                return &self.first_arenas_set;
            }

            const slot = self.handler_slot;
            if (cached_thread_arenas[slot]) |arena| {
                if (arena.tryAcquire()) {
                    return arena;
                }
            }

            const ptr = &thread_aid_counter[slot];
            const prev_v = @atomicRmw(u64, ptr, .Add, @bitCast(ArenaDispatcher{.index = 1, .capacity = 0}), .acquire);
            const dispatcher: ArenaDispatcher = @bitCast(prev_v);

            if (dispatcher.index == dispatcher_max_index) {
                _ = @atomicRmw(
                    u64, ptr, .Sub, @bitCast(ArenaDispatcher{.capacity = 0, .index = dispatcher_max_index}), .release
                );
            }

            return self.claimOrCreateArena(dispatcher);
        }

        inline fn claimOrCreateArena(self: *Self, dispatcher: ArenaDispatcher) ?*Arena {
            const index = dispatcher.index & (dispatcher.capacity - 1);
            const mod = index & batch_size_mask;
            const n_jumps = (index - mod) / config.shared_arena_batch_size;

            var opt_arenas_set: *ArenasSet = &self.first_arenas_set;
            for (0..n_jumps) |_| opt_arenas_set = opt_arenas_set.next.?;

            const arena = &opt_arenas_set.arenas[mod];
            if (!arena.tryAcquire()) {
                return self.createArena();
            }

            cached_thread_arenas[self.handler_slot] = arena;
            return arena;
        }

        fn createArena(self: *Self) ?*Arena {
            const mutex = &self.mutex;
            if (!mutex.tryLock()) {
                mutex.lock();
                mutex.unlock();

                return self.getArena();
            }
            defer mutex.unlock();

            const new_arenas_set = config.backing_allocator.create(ArenasSet) catch {
                return null;
            };
            new_arenas_set.* = ArenasSet.init();

            const last_arenas_set = self.last_arenas_set orelse &self.first_arenas_set;
            last_arenas_set.next = new_arenas_set;
            self.last_arenas_set = new_arenas_set;

            const arena = &new_arenas_set.arenas[0];
            assert(arena.tryAcquire());

            const new_dispatcher = ArenaDispatcher{
                .index = 0,
                .capacity = config.shared_arena_batch_size
            };
            _ = @atomicRmw(u64, &thread_aid_counter[self.handler_slot], .Add, @bitCast(new_dispatcher), .release);

            return arena;
        }
    };
}
