import XCTest
@testable import NisabaDB

final class BinJSONTests: XCTestCase {
    func testRoundTrip() throws {
        let oid = ObjectId()
        let value: BJValue = [
            "s": "héllo",
            "i": 42,
            "f": 1.5,
            "b": true,
            "n": nil,
            "arr": [1, "two", [3.0]],
            "oid": .objectId(oid),
            "when": .date(1_722_000_000_000),
            "nested": ["deep": ["x": 1]]
        ]
        let bytes = try BinJSON.encode(value)
        let back = try BinJSON.decode(bytes)
        XCTAssertEqual(back, value)
    }

    func testObjectIdHex() {
        let oid = ObjectId()
        XCTAssertEqual(ObjectId(hex: oid.hex), oid)
        XCTAssertEqual(oid.hex.count, 24)
        XCTAssertNil(ObjectId(hex: "nope"))
    }
}

final class NisabaTests: XCTestCase {
    private func tempRoot() -> String {
        NSTemporaryDirectory() + "nisaba-swift-test-" + UUID().uuidString
    }

    func testPing() throws {
        let root = tempRoot()
        defer { try? FileManager.default.removeItem(atPath: root) }
        let db = try Nisaba(rootPath: root)
        let res = try db.call(["op": "ping"])
        XCTAssertEqual(res["ok"]?.boolValue, true)
        db.shutdown()
    }

    func testInsertFindUpdateDelete() throws {
        let root = tempRoot()
        defer { try? FileManager.default.removeItem(atPath: root) }
        let db = try Nisaba(rootPath: root)
        defer { db.shutdown() }

        let id = ObjectId()
        let insert: BJValue = [
            "op": "insert", "db": "app", "coll": "users",
            "doc": ["_id": .objectId(id), "name": "ada", "age": 36],
            "id": .objectId(id)
        ]
        let ins = try db.call(insert)
        XCTAssertEqual(ins["ok"]?.boolValue, true)

        let found = try db.call([
            "op": "findOne", "db": "app", "coll": "users",
            "filter": ["name": "ada"]
        ])
        XCTAssertEqual(found["doc"]?["age"]?.intValue, 36)
        XCTAssertEqual(found["doc"]?["_id"], .objectId(id))

        let upd = try db.call([
            "op": "update", "db": "app", "coll": "users",
            "filter": ["name": "ada"],
            "update": ["$set": ["age": 37]],
            "now": .int(Int64(Date().timeIntervalSince1970 * 1000))
        ])
        XCTAssertEqual(upd["ok"]?.boolValue, true)

        let count = try db.call([
            "op": "count", "db": "app", "coll": "users",
            "filter": ["age": 37]
        ])
        XCTAssertEqual(count["n"]?.intValue, 1)

        let del = try db.call([
            "op": "delete", "db": "app", "coll": "users", "filter": [:]
        ])
        XCTAssertEqual(del["ok"]?.boolValue, true)
    }

    func testRefusalIsThrown() throws {
        let root = tempRoot()
        defer { try? FileManager.default.removeItem(atPath: root) }
        let db = try Nisaba(rootPath: root)
        defer { db.shutdown() }
        XCTAssertThrowsError(try db.call(["op": "definitelyNotAnOp"])) { err in
            guard case NisabaError.server = err else {
                return XCTFail("expected a server refusal, got \(err)")
            }
        }
    }

    func testPersistenceAcrossReopen() throws {
        let root = tempRoot()
        defer { try? FileManager.default.removeItem(atPath: root) }
        let id = ObjectId()
        do {
            let db = try Nisaba(rootPath: root)
            _ = try db.call([
                "op": "insert", "db": "app", "coll": "notes",
                "doc": ["_id": .objectId(id), "text": "survives"],
                "id": .objectId(id)
            ])
            db.shutdown()
        }
        let db = try Nisaba(rootPath: root)
        defer { db.shutdown() }
        let found = try db.call([
            "op": "findOne", "db": "app", "coll": "notes", "filter": [:]
        ])
        XCTAssertEqual(found["doc"]?["text"]?.stringValue, "survives")
    }
}
