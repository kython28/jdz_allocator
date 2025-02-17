const std = @import("std");
const span_arena = @import("arena.zig");
const jdz_allocator = @import("jdz_allocator.zig");
const utils = @import("utils.zig");

const JdzAllocConfig = jdz_allocator.JdzAllocConfig;

const assert = utils.assert;

const SlotState = enum {
    Available, Occupied
};

var slots_counter: usize = 0;
const MAX_SLOTS = 256;

var slots_state: [MAX_SLOTS]SlotState = @splat(SlotState.Available);
var thread_aid_counter: [MAX_SLOTS]u64 = @splat(0);

const ArenaDispatcher = packed struct {
    index: u32,
    capacity: u32,
};
const dispatcher_max_index = std.math.maxInt(u32)/2;

pub fn SharedArenaHandler(comptime config: JdzAllocConfig) type {
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

        first_arenas_set: ArenasSet,
        last_arenas_set: ?*ArenasSet = null,

        last_free_arena: ?*Arena = null,

        mutex: Mutex,
        arenas_batch: usize = 1,
        handler_slot: usize,

        const Self = @This();


        pub fn init() Self {
            const slot: usize = @atomicRmw(usize, &slots_counter, .Add, 1, .acquire) % MAX_SLOTS;
            if (@cmpxchgStrong(SlotState, &slots_state[slot], SlotState.Available, SlotState.Occupied, .acquire, .monotonic)) |_| {
                @panic("No free slots available");
            }

            thread_aid_counter[slot] = @bitCast(ArenaDispatcher{.capacity = config.shared_arena_batch_size, .index = 0});
            return .{
                .first_arenas_set = ArenasSet.init(),
                .mutex = .{},
                .handler_slot = slot,
            };
        }

        pub fn deinit(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();

            var spans_leaked: usize = 0;
            const first_arenas_set = &self.first_arenas_set;
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

            @atomicStore(SlotState, &slots_state[self.handler_slot], SlotState.Available, .release);

            @memset(&first_arenas_set.arenas, undefined);
            first_arenas_set.next = null;
            return spans_leaked;
        }

        pub fn getArena(self: *Self) ?*Arena {
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
            const index = dispatcher.index % dispatcher.capacity;
            const mod = index % config.shared_arena_batch_size;
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
