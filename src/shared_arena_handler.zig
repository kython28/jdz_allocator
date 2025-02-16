const std = @import("std");
const span_arena = @import("arena.zig");
const jdz_allocator = @import("jdz_allocator.zig");
const utils = @import("utils.zig");

const JdzAllocConfig = jdz_allocator.JdzAllocConfig;

const assert = std.debug.assert;

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
            const slot: usize = @atomicRmw(usize, &slots_counter, .Add, 1, .monotonic) % MAX_SLOTS;
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

            const arenas_batch = self.arenas_batch;
            if ((arenas_batch * config.shared_arena_batch_size) > tid) {
                mutex.unlock();
                return self.claimOrCreateArena(tid);
            }

            defer mutex.unlock();

            const new_arenas_set = config.backing_allocator.create(ArenasSet) catch {
                return null;
            };

            self.arenas_batch = arenas_batch + 1;

            for (&new_arenas_set.arenas) |*new_arena| {
                new_arena.* = Arena.init(.unlocked, null);
            }
            new_arenas_set.next = null;

            if (self.last_arenas_set) |*l_arena| {
                l_arena.*.next = new_arenas_set;
                l_arena.* = new_arenas_set;
            }else{
                self.first_arenas_set.next = new_arenas_set;
                self.last_arenas_set = new_arenas_set;
            }

            const f_arena = &new_arenas_set.arenas[0];
            f_arena.thread_id = @intCast(tid);

            return f_arena;
        }

        inline fn getThreadId(self: *Self, tid: *usize) bool {
            const slot = self.handler_slot;
            if (cached_thread_tids[slot]) |v| {
                tid.* = v - thread_tid_offsets[slot];
                return false;
            }

            const prev_v = @atomicRmw(usize, &thread_tid_counter[slot], .Add, 1, .monotonic);
            cached_thread_tids[slot] = prev_v;
            tid.* = prev_v - thread_tid_offsets[slot];
            return true;
        }
    };
}
