import Foundation

/// A MongoDB-style 12-byte ObjectId. Ids are a CLIENT-side concern in
/// nisaba-db (db.h's top comment): the engine never invents one, so this
/// side mints them — 4-byte big-endian seconds, 5 random bytes fixed per
/// process, 3-byte counter seeded randomly — the same layout MongoDB
/// drivers use.
public struct ObjectId: Equatable, Hashable, Sendable, CustomStringConvertible {
    public let bytes: [UInt8]   // always exactly 12

    private static let lock = NSLock()
    private static let processRandom: [UInt8] = (0..<5).map { _ in UInt8.random(in: 0...255) }
    private static var counter: UInt32 = UInt32.random(in: 0...0xFFFFFF)

    /// A fresh id stamped with the current time.
    public init() {
        let seconds = UInt32(clamping: Int64(Date().timeIntervalSince1970))
        Self.lock.lock()
        Self.counter = (Self.counter &+ 1) & 0xFFFFFF
        let count = Self.counter
        Self.lock.unlock()
        var b = [UInt8]()
        b.reserveCapacity(12)
        b.append(UInt8(truncatingIfNeeded: seconds >> 24))
        b.append(UInt8(truncatingIfNeeded: seconds >> 16))
        b.append(UInt8(truncatingIfNeeded: seconds >> 8))
        b.append(UInt8(truncatingIfNeeded: seconds))
        b.append(contentsOf: Self.processRandom)
        b.append(UInt8(truncatingIfNeeded: count >> 16))
        b.append(UInt8(truncatingIfNeeded: count >> 8))
        b.append(UInt8(truncatingIfNeeded: count))
        self.bytes = b
    }

    public init?(bytes: [UInt8]) {
        guard bytes.count == 12 else { return nil }
        self.bytes = bytes
    }

    /// From the 24-character hex form ObjectId("...") prints.
    public init?(hex: String) {
        guard hex.count == 24 else { return nil }
        var b = [UInt8]()
        b.reserveCapacity(12)
        var it = hex.startIndex
        for _ in 0..<12 {
            let next = hex.index(it, offsetBy: 2)
            guard let byte = UInt8(hex[it..<next], radix: 16) else { return nil }
            b.append(byte)
            it = next
        }
        self.bytes = b
    }

    public var hex: String {
        bytes.map { String(format: "%02x", $0) }.joined()
    }

    public var description: String { "ObjectId(\"\(hex)\")" }
}
