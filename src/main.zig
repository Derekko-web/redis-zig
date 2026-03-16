const std = @import("std");
const stdout = std.fs.File.stdout();
const net = std.net;

const RespCommand = struct {
    name: []const u8,
    arg: ?[]const u8,
};

pub fn main() !void {
    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.writeAll("accepted new connection\n");

        const thread = try std.Thread.spawn(.{}, handleConnection, .{connection});
        thread.detach();
    }
}

fn handleConnection(connection: std.net.Server.Connection) !void {
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
            const message = command.arg orelse continue;
            var header_buffer: [32]u8 = undefined;
            const header = try std.fmt.bufPrint(&header_buffer, "${d}\r\n", .{message.len});

            try connection.stream.writeAll(header);
            try connection.stream.writeAll(message);
            try connection.stream.writeAll("\r\n");
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

    const name = nextBulkString(&lines) orelse return null;
    const arg = if (element_count > 1) nextBulkString(&lines) else null;

    return .{
        .name = name,
        .arg = arg,
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
