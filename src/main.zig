const std = @import("std");
const stdout = std.fs.File.stdout();
const net = std.net;
const max_command_args = 64;

const RespCommand = struct {
    name: []const u8,
    args: [max_command_args][]const u8,
    arg_count: usize,
};

const Entry = struct {
    key: []u8,
    value: []u8,
    expires_at_ms: ?i64,
};

const ListEntry = struct {
    key: []u8,
    value: []u8,
};

const BlpopWaiter = struct {
    key: []u8,
    mutex: std.Thread.Mutex = .{},
    condition: std.Thread.Condition = .{},
    ready: bool = false,
    value: ?[]u8 = null,
};

const Database = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(Entry),
    lists: std.ArrayList(ListEntry),
    waiters: std.ArrayList(*BlpopWaiter),
    mutex: std.Thread.Mutex = .{},

    fn init(allocator: std.mem.Allocator) Database {
        return .{
            .allocator = allocator,
            .entries = .empty,
            .lists = .empty,
            .waiters = .empty,
        };
    }

    fn set(self: *Database, key: []const u8, value: []const u8, expires_at_ms: ?i64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.entries.items) |*entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                self.allocator.free(entry.value);
                entry.value = try self.allocator.dupe(u8, value);
                entry.expires_at_ms = expires_at_ms;
                return;
            }
        }

        try self.entries.append(self.allocator, .{
            .key = try self.allocator.dupe(u8, key),
            .value = try self.allocator.dupe(u8, value),
            .expires_at_ms = expires_at_ms,
        });
    }

    fn get(self: *Database, key: []const u8) ?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.entries.items) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                if (entry.expires_at_ms) |expires_at_ms| {
                    if (std.time.milliTimestamp() >= expires_at_ms) {
                        return null;
                    }
                }

                return entry.value;
            }
        }

        return null;
    }

    fn rpush(self: *Database, key: []const u8, values: []const []const u8) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var list_len: usize = 0;
        for (self.lists.items) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                list_len += 1;
            }
        }

        var start_index: usize = 0;
        if (values.len > 0 and try self.wakeBlpopWaiterLocked(key, values[0])) {
            start_index = 1;
        }

        for (values[start_index..]) |value| {
            try self.lists.append(self.allocator, .{
                .key = try self.allocator.dupe(u8, key),
                .value = try self.allocator.dupe(u8, value),
            });
        }

        return list_len + values.len;
    }

    fn lpush(self: *Database, key: []const u8, values: []const []const u8) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var list_len: usize = 0;
        var insert_index = self.lists.items.len;
        var found_first = false;

        for (self.lists.items, 0..) |entry, index| {
            if (std.mem.eql(u8, entry.key, key)) {
                list_len += 1;

                if (!found_first) {
                    insert_index = index;
                    found_first = true;
                }
            }
        }

        for (values) |value| {
            try self.lists.insert(self.allocator, insert_index, .{
                .key = try self.allocator.dupe(u8, key),
                .value = try self.allocator.dupe(u8, value),
            });
        }

        return list_len + values.len;
    }

    fn llen(self: *Database, key: []const u8) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var list_len: usize = 0;
        for (self.lists.items) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                list_len += 1;
            }
        }

        return list_len;
    }

    fn wakeBlpopWaiterLocked(self: *Database, key: []const u8, value: []const u8) !bool {
        for (self.waiters.items, 0..) |waiter, index| {
            if (std.mem.eql(u8, waiter.key, key)) {
                _ = self.waiters.orderedRemove(index);

                waiter.mutex.lock();
                defer waiter.mutex.unlock();

                waiter.value = try self.allocator.dupe(u8, value);
                waiter.ready = true;
                waiter.condition.signal();
                return true;
            }
        }

        return false;
    }

    fn removeBlpopWaiter(self: *Database, waiter: *BlpopWaiter) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.waiters.items, 0..) |queued_waiter, index| {
            if (queued_waiter == waiter) {
                _ = self.waiters.orderedRemove(index);
                return true;
            }
        }

        return false;
    }

    fn beginBlpop(self: *Database, key: []const u8, waiter: *BlpopWaiter) !?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.lists.items, 0..) |entry, index| {
            if (std.mem.eql(u8, entry.key, key)) {
                const removed = self.lists.orderedRemove(index);
                self.allocator.free(removed.key);
                return removed.value;
            }
        }

        try self.waiters.append(self.allocator, waiter);
        return null;
    }

    fn lpop(self: *Database, key: []const u8) ?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.lists.items, 0..) |entry, index| {
            if (std.mem.eql(u8, entry.key, key)) {
                const removed = self.lists.orderedRemove(index);
                self.allocator.free(removed.key);
                return removed.value;
            }
        }

        return null;
    }

    fn writeLRange(self: *Database, stream: anytype, key: []const u8, start: i64, stop: i64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var list_len: usize = 0;
        for (self.lists.items) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                list_len += 1;
            }
        }

        if (list_len == 0) {
            try stream.writeAll("*0\r\n");
            return;
        }

        const list_len_i64: i64 = @intCast(list_len);
        var resolved_start = start;
        if (resolved_start < 0) {
            resolved_start += list_len_i64;
        }
        if (resolved_start < 0) {
            resolved_start = 0;
        }

        var resolved_stop = stop;
        if (resolved_stop < 0) {
            resolved_stop += list_len_i64;
        }
        if (resolved_stop < 0) {
            resolved_stop = 0;
        }

        if (resolved_start >= list_len_i64 or resolved_start > resolved_stop) {
            try stream.writeAll("*0\r\n");
            return;
        }

        if (resolved_stop >= list_len_i64) {
            resolved_stop = list_len_i64 - 1;
        }

        const range_start: usize = @intCast(resolved_start);
        const range_end: usize = @intCast(resolved_stop);
        const result_len = range_end - range_start + 1;

        var header_buffer: [32]u8 = undefined;
        const header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{result_len});
        try stream.writeAll(header);

        var list_index: usize = 0;
        for (self.lists.items) |entry| {
            if (!std.mem.eql(u8, entry.key, key)) {
                continue;
            }

            if (list_index >= range_start and list_index <= range_end) {
                try writeBulkString(stream, entry.value);
            }

            list_index += 1;
            if (list_index > range_end) {
                break;
            }
        }
    }
};

pub fn main() !void {
    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var database = Database.init(std.heap.page_allocator);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.writeAll("accepted new connection\n");

        const thread = try std.Thread.spawn(.{}, handleConnection, .{ connection, &database });
        thread.detach();
    }
}

fn handleConnection(connection: std.net.Server.Connection, database: *Database) !void {
    defer connection.stream.close();

    var buffer: [1024]u8 = undefined;
    while (true) {
        const bytes_read = connection.stream.read(&buffer) catch 0;
        if (bytes_read == 0) {
            break;
        }

        const command = parseCommand(buffer[0..bytes_read]) orelse continue;

        if (std.ascii.eqlIgnoreCase(command.name, "ping")) {
            try connection.stream.writeAll("+PONG\r\n");
        } else if (std.ascii.eqlIgnoreCase(command.name, "echo")) {
            if (command.arg_count < 1) continue;
            const message = command.args[0];
            try writeBulkString(connection.stream, message);
        } else if (std.ascii.eqlIgnoreCase(command.name, "set")) {
            if (command.arg_count < 2) continue;
            const key = command.args[0];
            const value = command.args[1];
            var expires_at_ms: ?i64 = null;

            if (command.arg_count > 2) {
                if (command.arg_count < 4) continue;
                const option = command.args[2];
                const option_value = command.args[3];

                if (!std.ascii.eqlIgnoreCase(option, "px")) {
                    continue;
                }

                const ttl_ms = std.fmt.parseInt(i64, option_value, 10) catch continue;
                expires_at_ms = std.time.milliTimestamp() + ttl_ms;
            }

            try database.set(key, value, expires_at_ms);
            try connection.stream.writeAll("+OK\r\n");
        } else if (std.ascii.eqlIgnoreCase(command.name, "get")) {
            if (command.arg_count < 1) continue;
            const key = command.args[0];
            const value = database.get(key) orelse {
                try connection.stream.writeAll("$-1\r\n");
                continue;
            };

            try writeBulkString(connection.stream, value);
        } else if (std.ascii.eqlIgnoreCase(command.name, "rpush")) {
            if (command.arg_count < 2) continue;
            const key = command.args[0];

            const list_len = try database.rpush(key, command.args[1..command.arg_count]);

            var integer_buffer: [32]u8 = undefined;
            const integer = try std.fmt.bufPrint(&integer_buffer, ":{d}\r\n", .{list_len});
            try connection.stream.writeAll(integer);
        } else if (std.ascii.eqlIgnoreCase(command.name, "lpush")) {
            if (command.arg_count < 2) continue;
            const key = command.args[0];

            const list_len = try database.lpush(key, command.args[1..command.arg_count]);

            var integer_buffer: [32]u8 = undefined;
            const integer = try std.fmt.bufPrint(&integer_buffer, ":{d}\r\n", .{list_len});
            try connection.stream.writeAll(integer);
        } else if (std.ascii.eqlIgnoreCase(command.name, "lrange")) {
            if (command.arg_count < 3) continue;
            const key = command.args[0];
            const start = std.fmt.parseInt(i64, command.args[1], 10) catch continue;
            const stop = std.fmt.parseInt(i64, command.args[2], 10) catch continue;

            try database.writeLRange(connection.stream, key, start, stop);
        } else if (std.ascii.eqlIgnoreCase(command.name, "llen")) {
            if (command.arg_count < 1) continue;
            const key = command.args[0];
            const list_len = database.llen(key);

            var integer_buffer: [32]u8 = undefined;
            const integer = try std.fmt.bufPrint(&integer_buffer, ":{d}\r\n", .{list_len});
            try connection.stream.writeAll(integer);
        } else if (std.ascii.eqlIgnoreCase(command.name, "lpop")) {
            if (command.arg_count < 1) continue;
            const key = command.args[0];

            if (command.arg_count > 1) {
                const count = std.fmt.parseInt(usize, command.args[1], 10) catch continue;
                var values: std.ArrayList([]u8) = .empty;
                defer {
                    for (values.items) |value| {
                        database.allocator.free(value);
                    }
                    values.deinit(database.allocator);
                }

                var index: usize = 0;
                while (index < count) : (index += 1) {
                    const value = database.lpop(key) orelse break;
                    try values.append(database.allocator, value);
                }

                var header_buffer: [32]u8 = undefined;
                const header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{values.items.len});
                try connection.stream.writeAll(header);

                for (values.items) |value| {
                    try writeBulkString(connection.stream, value);
                }
            } else {
                const value = database.lpop(key) orelse {
                    try connection.stream.writeAll("$-1\r\n");
                    continue;
                };

                defer database.allocator.free(value);
                try writeBulkString(connection.stream, value);
            }
        } else if (std.ascii.eqlIgnoreCase(command.name, "blpop")) {
            if (command.arg_count < 2) continue;
            const key = command.args[0];
            const timeout_seconds = std.fmt.parseFloat(f64, command.args[1]) catch continue;
            if (timeout_seconds < 0) continue;

            if (timeout_seconds > 0) {
                const timeout_ms: i64 = @intFromFloat(timeout_seconds * 1000.0);
                const deadline = std.time.milliTimestamp() + timeout_ms;
                var got_value = false;

                while (std.time.milliTimestamp() < deadline) {
                    if (database.lpop(key)) |value| {
                        defer database.allocator.free(value);
                        try writeBlpopResponse(connection.stream, key, value);
                        got_value = true;
                        break;
                    }

                    std.Thread.sleep(std.time.ns_per_ms);
                }

                if (!got_value) {
                    if (database.lpop(key)) |value| {
                        defer database.allocator.free(value);
                        try writeBlpopResponse(connection.stream, key, value);
                    } else {
                        try connection.stream.writeAll("*-1\r\n");
                    }
                }
                continue;
            }

            var waiter = BlpopWaiter{
                .key = try database.allocator.dupe(u8, key),
            };
            defer database.allocator.free(waiter.key);

            waiter.mutex.lock();
            defer waiter.mutex.unlock();

            if (try database.beginBlpop(key, &waiter)) |value| {
                defer database.allocator.free(value);
                try writeBlpopResponse(connection.stream, key, value);
                continue;
            }

            while (!waiter.ready) {
                waiter.condition.wait(&waiter.mutex);
            }

            const value = waiter.value orelse continue;
            defer database.allocator.free(value);
            try writeBlpopResponse(connection.stream, waiter.key, value);
        }
    }
}

fn parseCommand(data: []const u8) ?RespCommand {
    var lines = std.mem.splitSequence(u8, data, "\r\n");

    const array_header = lines.next() orelse return null;
    if (array_header.len < 2 or array_header[0] != '*') {
        return null;
    }

    const element_count = std.fmt.parseInt(usize, array_header[1..], 10) catch return null;
    if (element_count == 0) {
        return null;
    }

    if (element_count - 1 > max_command_args) {
        return null;
    }

    const name = nextBulkString(&lines) orelse return null;
    var args: [max_command_args][]const u8 = undefined;
    var arg_count: usize = 0;

    while (arg_count < element_count - 1) : (arg_count += 1) {
        args[arg_count] = nextBulkString(&lines) orelse return null;
    }

    return .{
        .name = name,
        .args = args,
        .arg_count = arg_count,
    };
}

fn nextBulkString(lines: anytype) ?[]const u8 {
    const bulk_header = lines.next() orelse return null;
    if (bulk_header.len < 2 or bulk_header[0] != '$') {
        return null;
    }

    const expected_len = std.fmt.parseInt(usize, bulk_header[1..], 10) catch return null;
    const value = lines.next() orelse return null;
    if (value.len != expected_len) {
        return null;
    }

    return value;
}

fn writeBulkString(stream: anytype, value: []const u8) !void {
    var header_buffer: [32]u8 = undefined;
    const header = try std.fmt.bufPrint(&header_buffer, "${d}\r\n", .{value.len});

    try stream.writeAll(header);
    try stream.writeAll(value);
    try stream.writeAll("\r\n");
}

fn writeBlpopResponse(stream: anytype, key: []const u8, value: []const u8) !void {
    try stream.writeAll("*2\r\n");
    try writeBulkString(stream, key);
    try writeBulkString(stream, value);
}
