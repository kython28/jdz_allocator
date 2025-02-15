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
        first_arena: ?*Arena,
        last_arena: ?*Arena,

        first_free_arena: ?*Arena,
        last_free_arena: ?*Arena,

        mutex: Mutex,
        arenas: usize = 0,
        free_arenas: usize = 0,

        const Self = @This();

        pub fn init() Self {
            return .{
                .first_arena = null,
                .last_arena = null,
                .first_free_arena = null,
                .last_free_arena = null,
                .mutex = .{},
            };
        }

        pub fn deinit(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();

            var spans_leaked: usize = 0;
            var opt_arena = self.first_arena;

            while (opt_arena) |arena| {
                const next = arena.next;

                spans_leaked += arena.deinit();

                opt_arena = next;
            }

            while (opt_arena) |arena| {
                const next = arena.next;

                if (arena.is_alloc_master) {
                    config.backing_allocator.destroy(arena);
                }

                opt_arena = next;
            }

            return spans_leaked;
        }

        pub fn getArena(self: *Self) ?*Arena {
            const tid = getThreadId();

            const mutex = &self.mutex;

            var first_arena: ?*Arena = undefined;
            var first_free_arena: ?*Arena = undefined;
            var arenas: usize = undefined;
            var free_arenas: usize = undefined;

            {
                mutex.lock();
                defer mutex.unlock();

                first_arena = self.first_arena;
                first_free_arena = self.first_free_arena;
                arenas = self.arenas;
                free_arenas = self.free_arenas;
            }

            if (first_arena) |f_arena| {
                return findOwnedThreadArena(tid, f_arena, arenas) orelse
                    self.claimOrCreateArena(
                        tid, arenas, free_arenas, f_arena, first_free_arena
                    );
            }else{
                return self.createArena(arenas);
            }
        }

        inline fn findOwnedThreadArena(tid: std.Thread.Id, first_arena: *Arena, arenas: usize) ?*Arena {
            var opt_arena = first_arena;
            for (1..arenas) |_| {
                if (opt_arena.thread_id == tid) {
                    return acquireArena(opt_arena, tid) orelse continue;
                }

                opt_arena = opt_arena.next.?;
            }

            if (opt_arena.thread_id == tid) {
                return acquireArena(opt_arena, tid);
            }

            return null;
        }

        inline fn claimArena(tid: std.Thread.Id, arenas: usize, first_arena: *Arena) ?*Arena {
            var opt_arena = first_arena;
            for (1..arenas) |_| {
                return acquireArena(opt_arena, tid) orelse {
                    opt_arena = opt_arena.next.?;
                    continue;
                };
            }

            return acquireArena(opt_arena, tid);
        }

        inline fn claimOrCreateArena(
            self: *Self, tid: std.Thread.Id, arenas: usize, free_arenas: usize,
            first_arena: *Arena, first_free_arena: ?*Arena
        ) ?*Arena {
            if (first_free_arena) |ff_arena| {
                if (claimArena(tid, free_arenas, ff_arena)) |arena| {
                    const mutex = &self.mutex;
                    mutex.lock();
                    defer mutex.unlock();

                    self.free_arenas -= 1;
                    self.last_arena = arena;
                    if (arena.next) |ptr| {
                        self.first_free_arena = ptr;
                    }else{
                        self.first_free_arena = null;
                        self.last_free_arena = null;
                    }

                    return arena;
                }
            }

            return claimArena(tid, arenas, first_arena) orelse self.createArena(arenas);
        }

        fn createArena(self: *Self, prev_arena_batch: usize) ?*Arena {
            const mutex = &self.mutex;
            mutex.lock();

            if (self.arenas != prev_arena_batch) {
                mutex.unlock();
                return self.getArena();
            }

            defer mutex.unlock();

            const new_arenas = config.backing_allocator.alloc(Arena, config.shared_arena_batch_size) catch {
                return null;
            };

            self.arenas += config.shared_arena_batch_size;

            const first_arena = &new_arenas[0];
            var prev_arena = first_arena;
            prev_arena.* = Arena.init(.unlocked, null);

            for (new_arenas[1..]) |*new_arena| {
                new_arena.* = Arena.init(.unlocked, null);
                prev_arena.next = new_arena;
                prev_arena = new_arena;
            }

            new_arenas[0].makeMaster();
            const acquired = acquireArena(&new_arenas[0], getThreadId()).?;

            if (self.last_arena) |*l_arena| {
                l_arena.*.next = first_arena;
                l_arena.* = first_arena;
            }else{
                self.first_arena = first_arena;
                self.last_arena = first_arena;
            }

            if (config.shared_arena_batch_size > 1) {
                self.free_arenas += config.shared_arena_batch_size - 1;
                if (self.last_free_arena) |*lf_arena| {
                    lf_arena.*.next = &new_arenas[1];
                    lf_arena.* = prev_arena;
                }else{
                    self.first_free_arena = &new_arenas[1];
                    self.last_free_arena = prev_arena;
                }
            }

            return acquired;
        }

        fn addArenaToList(self: *Self, new_arena: *Arena) void {
            if (self.first_arena == null) {
                self.first_arena = new_arena;

                return;
            }

            const arena = self.last_arena.?;
            arena.next = new_arena;
            self.last_arena = new_arena;
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
