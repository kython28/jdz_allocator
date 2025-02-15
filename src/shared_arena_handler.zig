const std = @import("std");
const span_arena = @import("arena.zig");
const jdz_allocator = @import("jdz_allocator.zig");
const utils = @import("utils.zig");

const JdzAllocConfig = jdz_allocator.JdzAllocConfig;

const assert = std.debug.assert;

threadlocal var cached_thread_id: ?std.Thread.Id = null;

pub fn SharedArenaHandler(comptime config: JdzAllocConfig) type {
    const Arena = span_arena.Arena(config, false);

    const Mutex = utils.getMutexType(config);

    return struct {
        const ArenasSet = struct {
            arenas: [config.shared_arena_batch_size]Arena,
            next: ?*ArenasSet
        };

        first_arenas_set: ?*ArenasSet,
        last_arenas_set: ?*ArenasSet,

        mutex: Mutex,
        arenas_batch: usize = 0,

        const Self = @This();

        pub fn init() Self {
            return .{
                .first_arenas_set = null,
                .last_arenas_set = null,
                .mutex = .{},
            };
        }

        pub fn deinit(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();

            var spans_leaked: usize = 0;
            var opt_arenas_set = self.first_arenas_set;

            while (opt_arenas_set) |arenas_set| {
                const next = arenas_set.next;

                for (&arenas_set.arenas) |*arena| {
                    spans_leaked += arena.deinit();
                }

                opt_arenas_set = next;
            }

            // Unreachable code?
            // while (opt_arenas_set) |arenas_set| {
            //     const next = arenas_set.next;

            //     if (arena.is_alloc_master) {
            //         config.backing_allocator.destroy(arenas_set);
            //     }

            //     opt_arenas_set = next;
            // }

            return spans_leaked;
        }

        pub fn getArena(self: *Self) ?*Arena {
            const tid = getThreadId();

            const mutex = &self.mutex;

            var first_arenas_set: ?*ArenasSet = undefined;
            var arenas: usize = undefined;

            {
                mutex.lock();
                defer mutex.unlock();

                first_arenas_set = self.first_arenas_set;
                arenas = self.arenas_batch;
            }

            if (first_arenas_set) |f_arenas| {
                return findOwnedThreadArena(tid, f_arenas, arenas) orelse
                    self.claimOrCreateArena(tid, arenas, f_arenas);
            }else{
                return self.createArena(tid, arenas);
            }
        }

        inline fn findOwnedThreadArena(tid: std.Thread.Id, first_arenas: *ArenasSet, arenas_batch: usize) ?*Arena {
            var opt_arenas_set: *ArenasSet = first_arenas;
            for (1..arenas_batch) |_| {
                for (&opt_arenas_set.arenas) |*arena| {
                    if (arena.thread_id == tid) {
                        return acquireArena(arena, tid) orelse continue;
                    }
                }

                opt_arenas_set = opt_arenas_set.next.?;
            }

            for (&opt_arenas_set.arenas) |*arena| {
                if (arena.thread_id == tid) {
                    return acquireArena(arena, tid) orelse continue;
                }
            }

            return null;
        }

        inline fn claimOrCreateArena(
            self: *Self, tid: std.Thread.Id, arenas_batch: usize, first_arenas: *ArenasSet
        ) ?*Arena {
            var opt_arenas_set: *ArenasSet = first_arenas;
            for (1..arenas_batch) |_| {
                for (&opt_arenas_set.arenas) |*arena| {
                    return acquireArena(arena, tid) orelse {
                        opt_arenas_set = opt_arenas_set.next.?;
                        continue;
                    };
                }

                opt_arenas_set = opt_arenas_set.next.?;
            }

            for (&opt_arenas_set.arenas) |*arena| {
                return acquireArena(arena, tid) orelse {
                    opt_arenas_set = opt_arenas_set.next.?;
                    continue;
                };
            }

            return self.createArena(tid, arenas_batch);
        }

        fn createArena(self: *Self, tid: std.Thread.Id, prev_arena_batch: usize) ?*Arena {
            const mutex = &self.mutex;
            mutex.lock();

            if (self.arenas_batch != prev_arena_batch) {
                mutex.unlock();
                return self.getArena();
            }

            defer mutex.unlock();

            const new_arenas_set = config.backing_allocator.create(ArenasSet) catch {
                return null;
            };


            self.arenas_batch += 1;

            for (&new_arenas_set.arenas) |*new_arena| {
                new_arena.* = Arena.init(.unlocked, null);
                new_arena.thread_id = tid;
            }
            new_arenas_set.next = null;

            const first_arena = &new_arenas_set.arenas[0];
            first_arena.makeMaster();
            const acquired = acquireArena(first_arena, getThreadId()).?;

            if (self.last_arenas_set) |*l_arena| {
                l_arena.*.next = new_arenas_set;
                l_arena.* = new_arenas_set;
            }else{
                self.first_arenas_set = new_arenas_set;
                self.last_arenas_set = new_arenas_set;
            }

            return acquired;
        }

        inline fn acquireArena(arena: *Arena, tid: std.Thread.Id) ?*Arena {
            if (arena.tryAcquire()) {
                arena.thread_id = tid;

                return arena;
            }

            return null;
        }

        inline fn getThreadId() std.Thread.Id {
            return cached_thread_id orelse {
                cached_thread_id = std.Thread.getCurrentId();

                return cached_thread_id.?;
            };
        }
    };
}
