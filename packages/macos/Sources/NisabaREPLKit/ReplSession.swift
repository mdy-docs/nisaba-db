import Foundation
import NisabaDB

/// Executes parsed REPL lines against one embedded Nisaba instance.
///
/// An actor because the engine is single-threaded C: every request walks
/// through here one at a time, off the main thread. Request shapes follow
/// src/db-server-client.js — the JS driver for the same wire — so the two
/// front ends can never disagree about what, say, updateOne sends.
public actor ReplSession {
    public struct Output {
        public let text: String
        public let isError: Bool
    }

    public nonisolated let rootPath: String
    private(set) var currentDb: String
    private var nisaba: Nisaba?

    /// One REPL, one client token: cursor/stream ownership needs a stable
    /// id, and this process is the only client.
    private let client: UInt64 = 1

    public init(rootPath: String, initialDb: String = "test") {
        self.rootPath = rootPath
        self.currentDb = initialDb
    }

    public func databaseName() -> String { currentDb }

    private func instance() throws -> Nisaba {
        if let nisaba { return nisaba }
        let opened = try Nisaba(rootPath: rootPath)
        nisaba = opened
        return opened
    }

    // ---- entry point -----------------------------------------------------

    public func execute(_ line: String) -> Output {
        do {
            guard let command = try ShellCommand.parse(line) else {
                return Output(text: "", isError: false)
            }
            return try run(command)
        } catch let e as ShellCommand.CommandError {
            return Output(text: e.description, isError: true)
        } catch let e as NisabaError {
            return Output(text: "Error: \(e.description)", isError: true)
        } catch {
            return Output(text: "Error: \(error)", isError: true)
        }
    }

    private func run(_ command: ShellCommand) throws -> Output {
        switch command {
        case .help:
            return Output(text: Self.helpText, isError: false)
        case .clear:
            return Output(text: "", isError: false)   // the view handles it
        case .currentDb:
            return Output(text: currentDb, isError: false)
        case .use(let name):
            currentDb = name
            return Output(text: "switched to db \(name)", isError: false)
        case .showDatabases:
            let res = try instance().call(["op": "listDatabases"], client: client)
            let names = res["databases"]?.arrayValue?.compactMap { $0.stringValue } ?? []
            return Output(text: names.isEmpty ? "(none)" : names.joined(separator: "\n"),
                          isError: false)
        case .showCollections:
            let names = try collectionNames()
            return Output(text: names.isEmpty ? "(none)" : names.joined(separator: "\n"),
                          isError: false)
        case .dbMethod(let name, let args):
            return try runDbMethod(name, args)
        case .collMethod(let coll, let method, let args, let modifiers):
            return try runCollMethod(coll, method, args, modifiers)
        }
    }

    // ---- database-level methods -----------------------------------------

    private func runDbMethod(_ name: String, _ args: [BJValue]) throws -> Output {
        switch name {
        case "getCollectionNames", "listCollections":
            let names = try collectionNames()
            return ok(.array(names.map { .string($0) }))
        case "createCollection":
            guard case .string(let coll)? = args.first else {
                throw ShellCommand.CommandError.parse("db.createCollection('name')")
            }
            let res = try scoped(["op": "createCollection", "coll": .string(coll)])
            return ok(["ok": true, "created": res["created"] ?? .null])
        case "dropDatabase":
            let res = try instance().call(
                ["op": "dropDatabase", "db": .string(currentDb)], client: client)
            return ok(["ok": true, "dropped": res["dropped"] ?? .null])
        case "ping", "runCommand", "hello":
            let res = try instance().call(["op": "ping"], client: client)
            return ok(res)
        default:
            throw ShellCommand.CommandError.parse(
                "unknown database method db.\(name)(...) — try `help`")
        }
    }

    private func collectionNames() throws -> [String] {
        do {
            let res = try scoped(["op": "listCollections"])
            return res["collections"]?.arrayValue?.compactMap { $0.stringValue } ?? []
        } catch NisabaError.server(let code, _) where Self.meansAbsent(code) {
            return []
        }
    }

    // ---- collection methods ---------------------------------------------

    private func runCollMethod(_ coll: String, _ method: String,
                               _ args: [BJValue],
                               _ modifiers: [(name: String, args: [BJValue])]) throws -> Output {
        // Only find takes chained modifiers; refuse them elsewhere so a
        // typo'd chain is heard rather than dropped.
        if !modifiers.isEmpty && method != "find" {
            throw ShellCommand.CommandError.parse(
                ".\(modifiers[0].name)(...) chains only onto find(...)")
        }

        func req(_ op: String, _ fields: [(String, BJValue)]) -> BJValue {
            .object([("op", .string(op)), ("coll", .string(coll))] + fields)
        }

        switch method {
        case "__name":
            return Output(text: "\(currentDb).\(coll)", isError: false)

        case "find":
            let filter = args.first ?? [:]
            var opts: [(String, BJValue)] = []
            if args.count > 1, case .object = args[1] { opts.append(("projection", args[1])) }
            for (name, margs) in modifiers {
                switch name {
                case "sort" where !margs.isEmpty: opts.append(("sort", margs[0]))
                case "limit" where !margs.isEmpty: opts.append(("limit", margs[0]))
                case "skip" where !margs.isEmpty: opts.append(("skip", margs[0]))
                case "pretty", "toArray": break
                default:
                    throw ShellCommand.CommandError.parse(
                        "unsupported find modifier .\(name)(...)")
                }
            }
            let fields: [(String, BJValue)] = [("filter", filter)]
                + (opts.isEmpty ? [] : [("opts", .object(opts))])
            return try emptyMeansNone(op: "find") {
                let res = try self.scoped(req("find", fields))
                return Output(text: Renderer.renderDocs(res["docs"]?.arrayValue ?? []),
                              isError: false)
            }

        case "findOne":
            let filter = args.first ?? [:]
            return try emptyMeansNone(op: "findOne") {
                let res = try self.scoped(req("findOne", [("filter", filter)]))
                guard res["found"]?.isTruthy == true, let doc = res["doc"] else {
                    return Output(text: "null", isError: false)
                }
                return self.ok(doc)
            }

        case "countDocuments", "count", "estimatedDocumentCount":
            let filter = args.first ?? [:]
            return try emptyMeansNone(op: "count") {
                let res = try self.scoped(req("count", [("filter", filter)]))
                return self.ok(res["n"] ?? .int(0))
            }

        case "distinct":
            guard case .string(let field)? = args.first else {
                throw ShellCommand.CommandError.parse("db.\(coll).distinct('field', filter?)")
            }
            let filter = args.count > 1 ? args[1] : [:]
            return try emptyMeansNone(op: "distinct") {
                let res = try self.scoped(req("distinct",
                    [("field", .string(field)), ("filter", filter)]))
                return self.ok(res["values"] ?? .array([]))
            }

        case "aggregate":
            let stages = args.first ?? .array([])
            return try emptyMeansNone(op: "aggregate") {
                let res = try self.scoped(req("aggregate", [("stages", stages)]))
                return Output(text: Renderer.renderDocs(res["docs"]?.arrayValue ?? []),
                              isError: false)
            }

        case "explain":
            let filter = args.first ?? [:]
            let res = try scoped(req("explain", [("filter", filter)]))
            return ok(res["plan"] ?? .null)

        case "insertOne":
            guard case .object(let pairs)? = args.first else {
                throw ShellCommand.CommandError.parse("db.\(coll).insertOne({...})")
            }
            let (doc, id) = Self.withId(pairs)
            _ = try scoped(req("insert", [("doc", doc), ("id", .objectId(id))]))
            return ok(["acknowledged": true, "insertedId": .objectId(id)])

        case "insertMany":
            guard case .array(let items)? = args.first, !items.isEmpty else {
                throw ShellCommand.CommandError.parse("db.\(coll).insertMany([{...}, ...])")
            }
            let ordered = args.count > 1 ? (args[1]["ordered"]?.boolValue ?? true) : true
            var docs: [BJValue] = []
            var ids: [ObjectId] = []
            for item in items {
                guard case .object(let pairs) = item else {
                    throw ShellCommand.CommandError.parse("insertMany takes documents")
                }
                let (doc, id) = Self.withId(pairs)
                docs.append(doc)
                ids.append(id)
            }
            let res = try scoped(req("insertMany",
                [("docs", .array(docs)), ("ordered", .bool(ordered))]))
            let attempted = Int(res["attempted"]?.intValue ?? 0)
            let failed = Set((res["errors"]?.arrayValue ?? [])
                .compactMap { $0["index"]?.intValue.map(Int.init) })
            var insertedIds: [(String, BJValue)] = []
            for i in 0..<attempted where !failed.contains(i) {
                insertedIds.append((String(i), .objectId(ids[i])))
            }
            var summary: [(String, BJValue)] = [
                ("acknowledged", .bool(true)),
                ("insertedCount", .int(Int64(insertedIds.count))),
                ("insertedIds", .object(insertedIds))
            ]
            if let errors = res["errors"], !(errors.arrayValue ?? []).isEmpty {
                summary.append(("writeErrors", errors))
            }
            return ok(.object(summary))

        case "updateOne", "updateMany", "replaceOne":
            guard args.count >= 2 else {
                throw ShellCommand.CommandError.parse(
                    "db.\(coll).\(method)(filter, \(method == "replaceOne" ? "doc" : "update"), options?)")
            }
            let op = method == "updateOne" ? "update"
                   : method == "updateMany" ? "updateMany" : "replace"
            let payloadKey = method == "replaceOne" ? "doc" : "update"
            var fields: [(String, BJValue)] = [
                ("filter", args[0]), (payloadKey, args[1]), ("now", .int(Self.nowMs()))
            ]
            if args.count > 2, args[2]["upsert"]?.isTruthy == true {
                fields.append(("upsert", .bool(true)))
                fields.append(("id", .objectId(ObjectId())))
            }
            let res = try scoped(req(op, fields))
            return ok(res["result"] ?? res)

        case "deleteOne", "deleteMany":
            let filter = args.first ?? [:]
            let res = try scoped(req(method == "deleteOne" ? "delete" : "deleteMany",
                                     [("filter", filter)]))
            return ok(res["result"] ?? res)

        case "findOneAndUpdate", "findOneAndReplace", "findOneAndDelete":
            var fields: [(String, BJValue)] = [("filter", args.first ?? [:]),
                                               ("now", .int(Self.nowMs()))]
            if method != "findOneAndDelete" {
                guard args.count >= 2 else {
                    throw ShellCommand.CommandError.parse("db.\(coll).\(method)(filter, ...)")
                }
                fields.append((method == "findOneAndUpdate" ? "update" : "doc", args[1]))
                let options = args.count > 2 ? args[2] : .null
                if options["returnDocument"]?.stringValue == "after"
                    || options["returnNew"]?.isTruthy == true {
                    fields.append(("returnNew", .bool(true)))
                }
                if options["upsert"]?.isTruthy == true {
                    fields.append(("upsert", .bool(true)))
                    fields.append(("id", .objectId(ObjectId())))
                }
            }
            let res = try scoped(req(method, fields))
            guard res["found"]?.isTruthy == true, let doc = res["doc"] else {
                return Output(text: "null", isError: false)
            }
            return ok(doc)

        case "createIndex":
            guard let keys = args.first else {
                throw ShellCommand.CommandError.parse(
                    "db.\(coll).createIndex({field: 1, ...}, options?)")
            }
            var fields: [(String, BJValue)] = [("keys", keys)]
            if args.count > 1 { fields.append(("options", args[1])) }
            let res = try scoped(req("createIndex", fields))
            return ok(res["name"] ?? .null)

        case "dropIndex":
            guard case .string(let name)? = args.first else {
                throw ShellCommand.CommandError.parse("db.\(coll).dropIndex('name')")
            }
            _ = try scoped(req("dropIndex", [("index", .string(name))]))
            return ok(["ok": true])

        case "getIndexes", "listIndexes":
            let res = try scoped(req("listIndexes", []))
            return ok(res["indexes"] ?? .array([]))

        case "findByIndex":
            guard case .string(let name)? = args.first, args.count >= 2 else {
                throw ShellCommand.CommandError.parse(
                    "db.\(coll).findByIndex('name', [values...])")
            }
            let res = try scoped(req("findByIndex",
                [("index", .string(name)), ("values", args[1])]))
            return Output(text: Renderer.renderDocs(res["docs"]?.arrayValue ?? []),
                          isError: false)

        case "drop":
            let res = try scoped(req("dropCollection", []))
            return ok(res["dropped"] ?? .bool(false))

        case "compact":
            let res = try scoped(req("compact", []))
            return ok(res["result"] ?? res)

        case "pruneExpired":
            let res = try scoped(req("pruneExpired", [("now", .int(Self.nowMs()))]))
            return ok(res["deletedCount"] ?? .int(0))

        case "watch":
            throw ShellCommand.CommandError.parse(
                "change streams are not supported in this REPL")

        default:
            throw ShellCommand.CommandError.parse(
                "unknown collection method .\(method)(...) — try `help`")
        }
    }

    // ---- plumbing --------------------------------------------------------

    /// Send one request with the current database name attached — the same
    /// `db` field the JS client's scope() prepends to every call.
    private func scoped(_ request: BJValue) throws -> BJValue {
        guard case .object(let pairs) = request else {
            throw ShellCommand.CommandError.parse("internal: request must be an object")
        }
        return try instance().call(
            .object([("db", .string(currentDb))] + pairs), client: client)
    }

    /// Reads against a database or collection that does not exist yet
    /// answer as "nothing there", the mongosh experience, rather than as
    /// the engine's (accurate) refusal.
    private static func meansAbsent(_ code: Int64) -> Bool {
        code == -37 || code == -43   // DC_ERR_NO_COLLECTION, DC_ERR_NO_DATABASE
    }

    private func emptyMeansNone(op: String, _ body: () throws -> Output) throws -> Output {
        do {
            return try body()
        } catch NisabaError.server(let code, _) where Self.meansAbsent(code) {
            switch op {
            case "find", "aggregate": return Output(text: "(no documents)", isError: false)
            case "findOne": return Output(text: "null", isError: false)
            case "count": return ok(.int(0))
            case "distinct": return ok(.array([]))
            default: return Output(text: "null", isError: false)
            }
        }
    }

    private func ok(_ value: BJValue) -> Output {
        Output(text: Renderer.render(value), isError: false)
    }

    /// The document with an _id up front — the caller's if it had one,
    /// a fresh one otherwise — and the id an insert reports back.
    private static func withId(_ pairs: [(String, BJValue)]) -> (BJValue, ObjectId) {
        if let existing = pairs.first(where: { $0.0 == "_id" }) {
            if case .objectId(let oid) = existing.1 {
                return (.object(pairs), oid)
            }
            // A non-OID _id: send it anyway; the engine owns the refusal.
            return (.object(pairs), ObjectId())
        }
        let id = ObjectId()
        return (.object([("_id", .objectId(id))] + pairs), id)
    }

    private static func nowMs() -> Int64 {
        Int64(Date().timeIntervalSince1970 * 1000)
    }

    static let helpText = """
    Commands
      use <db>                     switch database (created on first write)
      show dbs                     list databases
      show collections             list collections in the current database
      db                           print the current database name
      clear                        clear the transcript
      help                         this text

    Collections — db.<collection>.<method>(...)
      find(filter?, projection?)   chain .sort({f:1}) .skip(n) .limit(n)
      findOne(filter?)             countDocuments(filter?)
      distinct('field', filter?)   aggregate([{$match:...}, {$group:...}])
      insertOne({...})             insertMany([{...}, ...], {ordered:true}?)
      updateOne(f, u, {upsert}?)   updateMany(f, u, {upsert}?)
      replaceOne(f, doc, opts?)    deleteOne(f?)  deleteMany(f?)
      findOneAndUpdate(f, u, o?)   findOneAndReplace(f, doc, o?)
      findOneAndDelete(f?)         explain(filter?)
      createIndex({f:1}, opts?)    dropIndex('name')  getIndexes()
      findByIndex('name', [...])   drop()  compact()  pruneExpired()

    Database — db.<method>(...)
      getCollectionNames()  createCollection('name')  dropDatabase()  ping()

    Values: {unquoted: keys}, 'single' or "double" quotes,
    ObjectId('...'), ISODate('2026-01-01'), new Date(), NumberLong(...)
    """
}
