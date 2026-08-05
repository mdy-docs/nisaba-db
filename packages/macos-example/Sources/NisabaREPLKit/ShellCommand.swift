import Foundation
import NisabaDB

/// One parsed REPL line, mongosh-shaped.
enum ShellCommand {
    case help
    case clear
    case use(String)
    case showDatabases
    case showCollections
    case currentDb
    /// db.method(args) — a database-level call (createCollection, ...).
    case dbMethod(name: String, args: [BJValue])
    /// db.<coll>.method(args) plus any chained modifiers like
    /// .sort({...}).limit(5) — only find honors modifiers.
    case collMethod(coll: String, method: String, args: [BJValue],
                    modifiers: [(name: String, args: [BJValue])])

    enum CommandError: Error, CustomStringConvertible {
        case parse(String)
        var description: String {
            if case .parse(let msg) = self { return msg }
            return "parse error"
        }
    }

    static func parse(_ line: String) throws -> ShellCommand? {
        let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty { return nil }

        switch trimmed {
        case "help": return .help
        case "clear", "cls": return .clear
        case "db": return .currentDb
        case "show dbs", "show databases": return .showDatabases
        case "show collections", "show tables": return .showCollections
        default: break
        }

        if trimmed.hasPrefix("use ") || trimmed == "use" {
            let name = trimmed.dropFirst(3).trimmingCharacters(in: .whitespaces)
            guard !name.isEmpty else { throw CommandError.parse("use <database>") }
            guard name.allSatisfy({ $0.isLetter || $0.isNumber || $0 == "_" || $0 == "-" }) else {
                throw CommandError.parse("database names here are letters, digits, _ and -")
            }
            return .use(name)
        }

        guard trimmed == "db" || trimmed.hasPrefix("db.") else {
            throw CommandError.parse(
                "unrecognized command — try `help`, `use <db>`, `show collections`, " +
                "or `db.<collection>.<method>(...)`")
        }

        var p = JSONishParser(trimmed)
        _ = p.identifier()   // "db"

        // Walk .segment.segment...(args): the segment that owns the "("
        // is the method; everything before it names the collection.
        var segments: [String] = []
        var method: String? = nil
        var args: [BJValue] = []
        while p.take(".") {
            guard let name = p.identifier() else {
                throw CommandError.parse("expected a name after '.'")
            }
            if p.take("(") {
                method = name
                do {
                    args = try p.argumentList()
                    try p.expect(")")
                } catch let e as JSONishParser.ParseError {
                    throw CommandError.parse(e.description)
                }
                break
            }
            segments.append(name)
        }
        guard let method else {
            if segments.count == 1 {
                // `db.users` alone: mongosh prints the collection name.
                return .collMethod(coll: segments[0], method: "__name",
                                   args: [], modifiers: [])
            }
            throw CommandError.parse("expected a method call, e.g. db.users.find({})")
        }

        // Chained modifiers: .sort({...}).limit(5).pretty()
        var modifiers: [(String, [BJValue])] = []
        while p.take(".") {
            guard let name = p.identifier() else {
                throw CommandError.parse("expected a name after '.'")
            }
            do {
                try p.expect("(")
                let margs = try p.argumentList()
                try p.expect(")")
                modifiers.append((name, margs))
            } catch let e as JSONishParser.ParseError {
                throw CommandError.parse(e.description)
            }
        }
        guard p.atEnd else {
            throw CommandError.parse("unexpected trailing input")
        }

        if segments.isEmpty {
            guard modifiers.isEmpty else {
                throw CommandError.parse("db.\(method)(...) takes no chained calls")
            }
            return .dbMethod(name: method, args: args)
        }
        return .collMethod(coll: segments.joined(separator: "."), method: method,
                           args: args, modifiers: modifiers)
    }
}
