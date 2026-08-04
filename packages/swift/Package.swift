// swift-tools-version:5.9
// The Swift embedding of nisaba-db: the same C engine the wasm build
// links, compiled natively, with a thin Swift layer for binjson values,
// ObjectIds, and the request loop (db_instance.h's dbi_handle).
//
// Sources/CNisaba is ASSEMBLED, not authored: from the repository root
// run ./packages/swift.build.sh to copy the native source closure out of
// the repository. See that script's comment for why the package
// directory is never a source of truth.
import PackageDescription
import Foundation

let packageDir = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
if !FileManager.default.fileExists(
    atPath: packageDir.appendingPathComponent("Sources/CNisaba/src").path) {
    fatalError("""
    Sources/CNisaba has not been assembled. From the repository root run:

        ./packages/swift.build.sh

    and then build this package again.
    """)
}

let package = Package(
    name: "NisabaDB",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [.library(name: "NisabaDB", targets: ["NisabaDB"])],
    targets: [
        .target(name: "CNisaba"),
        .target(name: "NisabaDB", dependencies: ["CNisaba"]),
        .testTarget(name: "NisabaDBTests", dependencies: ["NisabaDB"])
    ],
    cLanguageStandard: .c11
)
