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
var thread_tid_counter: [MAX_SLOTS]usize = @splat(0);
var thread_tid_offsets: [MAX_SLOTS]usize = @splat(0);
threadlocal var cached_thread_tids: [MAX_SLOTS]?usize = @splat(null);

pub fn SharedArenaHandler(comptime config: JdzAllocConfig) type {
    const Arena = span_arena.Arena(config, false);

    const Mutex = utils.getMutexType(config);

    return struct {
        const ArenasSet = struct {
            arenas: [config.shared_arena_batch_size]Arena,
            next: ?*ArenasSet
        };

        first_arenas_set: ArenasSet,
        last_arenas_set: ?*ArenasSet,

        mutex: Mutex,
        arenas_batch: usize = 1,
        handler_slot: usize,

        const Self = @This();


        pub fn init() Self {
            const slot: usize = @atomicRmw(usize, &slots_counter, .Add, 1, .acquire) % MAX_SLOTS;
            if (@cmpxchgStrong(SlotState, &slots_state[slot], SlotState.Available, SlotState.Occupied, .acquire, .monotonic)) |_| {
                @panic("No free slots available");
            }

            var set: ArenasSet = .{
                .arenas = undefined,
                .next = null
            };

            for (&set.arenas) |*arena| {
                arena.* = Arena.init(.unlocked, null);
            }

            thread_tid_offsets[slot] = thread_tid_counter[slot];
            return .{
                .first_arenas_set = set,
                .last_arenas_set = null,
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
            var tid: usize = undefined;
            const new_thread = self.getThreadId(&tid);

            if (new_thread and tid >= config.shared_arena_batch_size) {
                return self.createArena(tid);
            }

            return self.claimOrCreateArena(tid);
        }

        inline fn claimOrCreateArena(self: *Self, tid: usize) ?*Arena {
            const index = tid % config.shared_arena_batch_size;
            const n_jumps = (tid - index) / config.shared_arena_batch_size;
            var opt_arenas_set: *ArenasSet = &self.first_arenas_set;
            for (0..n_jumps) |_| opt_arenas_set = opt_arenas_set.next.?;

            return &opt_arenas_set.arenas[index];
        }

        fn createArena(self: *Self, tid: usize) ?*Arena {
            const mutex = &self.mutex;
            mutex.lock();

            var arenas_batch = self.arenas_batch;
            if ((arenas_batch * config.shared_arena_batch_size) > tid) {
                mutex.unlock();
                return self.claimOrCreateArena(tid);
            }

            defer mutex.unlock();

            var last_arenas_set = self.last_arenas_set orelse &self.first_arenas_set;
            defer self.last_arenas_set = last_arenas_set;

            const new_arenas_batch = (tid - (tid % config.shared_arena_batch_size)) / config.shared_arena_batch_size + 1;
            defer self.arenas_batch = arenas_batch;

            while (arenas_batch < new_arenas_batch) {
                const new_arenas_set = config.backing_allocator.create(ArenasSet) catch {
                    return null;
                };

                for (&new_arenas_set.arenas) |*new_arena| {
                    new_arena.* = Arena.init(.unlocked, null);
                }
                new_arenas_set.next = null;

                last_arenas_set.next = new_arenas_set;
                last_arenas_set = new_arenas_set;
                arenas_batch += 1;
            }

            const new_arena = &last_arenas_set.arenas[tid % config.shared_arena_batch_size];
            new_arena.thread_id = @intCast(tid);

            return new_arena;
        }

        inline fn getThreadId(self: *Self, tid: *usize) bool {
            const slot = self.handler_slot;
            const offset = thread_tid_offsets[slot];
            if (cached_thread_tids[slot]) |v| {
                if (v >= offset) {
                    tid.* = v - offset;
                    return false;
                }
            }

            const prev_v = @atomicRmw(usize, &thread_tid_counter[slot], .Add, 1, .acquire);
            cached_thread_tids[slot] = prev_v;
            tid.* = prev_v - offset;
            return true;
        }
    };
}
