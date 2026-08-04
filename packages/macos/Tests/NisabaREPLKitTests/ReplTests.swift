import XCTest
import NisabaDB
@testable import NisabaREPLKit

final class JSONishTests: XCTestCase {
    private func parse(_ s: String) throws -> BJValue {
        var p = JSONishParser(s)
        let v = try p.value()
        XCTAssertTrue(p.atEnd, "trailing input after \(s)")
        return v
    }

    func testRelaxedSyntax() throws {
        let v = try parse("{name: 'ada', age: 36, tags: [\"x\", 'y'], nested: {$gte: 2.5}}")
        XCTAssertEqual(v["name"], .string("ada"))
        XCTAssertEqual(v["age"], .int(36))
        XCTAssertEqual(v["tags"], .array([.string("x"), .string("y")]))
        XCTAssertEqual(v["nested"]?["$gte"], .double(2.5))
    }

    func testSpecialValues() throws {
        let v = try parse("{id: ObjectId('665f00000000000000000001'), when: ISODate('2026-01-02'), n: NumberLong(9), neg: -3, none: null}")
        XCTAssertEqual(v["id"], .objectId(ObjectId(hex: "665f00000000000000000001")!))
        XCTAssertEqual(v["when"], .date(1_767_312_000_000))
        XCTAssertEqual(v["n"], .int(9))
        XCTAssertEqual(v["neg"], .int(-3))
        XCTAssertEqual(v["none"], .null)
    }

    func testTrailingCommaAndEmpty() throws {
        XCTAssertEqual(try parse("{a: 1,}"), .object([("a", .int(1))]))
        XCTAssertEqual(try parse("[1, 2,]"), .array([.int(1), .int(2)]))
        XCTAssertEqual(try parse("{}"), .object([]))
    }
}

final class ShellCommandTests: XCTestCase {
    func testFindChain() throws {
        let c = try ShellCommand.parse("db.users.find({age: {$gt: 1}}).sort({name: 1}).limit(5)")
        guard case .collMethod(let coll, let method, let args, let modifiers)? = c else {
            return XCTFail("wrong shape")
        }
        XCTAssertEqual(coll, "users")
        XCTAssertEqual(method, "find")
        XCTAssertEqual(args.count, 1)
        XCTAssertEqual(modifiers.map(\.name), ["sort", "limit"])
    }

    func testDottedCollection() throws {
        guard case .collMethod(let coll, "findOne", _, _)? =
            try ShellCommand.parse("db.system.things.findOne()") else {
            return XCTFail("wrong shape")
        }
        XCTAssertEqual(coll, "system.things")
    }

    func testKeywords() throws {
        guard case .use("app")? = try ShellCommand.parse("use app") else {
            return XCTFail("use")
        }
        guard case .showCollections? = try ShellCommand.parse("show collections") else {
            return XCTFail("show collections")
        }
    }
}

final class ReplSessionTests: XCTestCase {
    private func makeSession() -> ReplSession {
        ReplSession(rootPath: NSTemporaryDirectory() + "nisaba-repl-test-" + UUID().uuidString)
    }

    func testEndToEnd() async throws {
        let session = makeSession()
        defer { try? FileManager.default.removeItem(atPath: session.rootPath) }

        var out = await session.execute("use app")
        XCTAssertEqual(out.text, "switched to db app")

        out = await session.execute("db.users.insertOne({name: 'ada', age: 36})")
        XCTAssertFalse(out.isError, out.text)
        XCTAssertTrue(out.text.contains("acknowledged: true"), out.text)
        XCTAssertTrue(out.text.contains("insertedId: ObjectId('"), out.text)

        _ = await session.execute("db.users.insertOne({name: 'grace', age: 45})")

        out = await session.execute("db.users.find({age: {$gte: 40}})")
        XCTAssertFalse(out.isError, out.text)
        XCTAssertTrue(out.text.contains("grace"), out.text)
        XCTAssertFalse(out.text.contains("ada"), out.text)

        out = await session.execute("db.users.find({}).sort({age: -1}).limit(1)")
        XCTAssertTrue(out.text.contains("grace"), out.text)

        out = await session.execute("db.users.updateOne({name: 'ada'}, {$set: {age: 37}})")
        XCTAssertFalse(out.isError, out.text)
        XCTAssertTrue(out.text.contains("matchedCount"), out.text)

        out = await session.execute("db.users.countDocuments({age: 37})")
        XCTAssertEqual(out.text, "1")

        out = await session.execute("show collections")
        XCTAssertEqual(out.text, "users")

        out = await session.execute("db.users.deleteMany({})")
        XCTAssertTrue(out.text.contains("deletedCount"), out.text)
    }

    func testAbsentReadsAnswerEmpty() async throws {
        let session = makeSession()
        defer { try? FileManager.default.removeItem(atPath: session.rootPath) }
        var out = await session.execute("db.nothing.find({})")
        XCTAssertEqual(out.text, "(no documents)")
        out = await session.execute("db.nothing.findOne()")
        XCTAssertEqual(out.text, "null")
        out = await session.execute("db.nothing.countDocuments()")
        XCTAssertEqual(out.text, "0")
        out = await session.execute("show collections")
        XCTAssertEqual(out.text, "(none)")
    }

    func testParseErrorIsFriendly() async throws {
        let session = makeSession()
        let out = await session.execute("db.users.find({age: })")
        XCTAssertTrue(out.isError)
    }

    func testUnknownOpRefusal() async throws {
        let session = makeSession()
        defer { try? FileManager.default.removeItem(atPath: session.rootPath) }
        let out = await session.execute("db.users.frobnicate({})")
        XCTAssertTrue(out.isError)
        XCTAssertTrue(out.text.contains("unknown collection method"), out.text)
    }
}
