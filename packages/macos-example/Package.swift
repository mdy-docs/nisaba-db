// swift-tools-version:5.9
// A minimal macOS SwiftUI REPL over the embedded nisaba-db engine
// (packages/swift): mongosh-style input, e.g.
//
//     use app
//     db.users.insertOne({name: "ada", age: 36})
//     db.users.find({age: {$gte: 21}}).sort({name: 1}).limit(5)
//
// Run with `swift run` from this directory (packages/swift must be
// assembled first: ./packages/swift.build.sh from the repo root).
//
// NisabaREPLKit holds everything with no window in it — the mongosh-ish
// parser, the request mapping, the renderer, the session actor — so the
// tests exercise exactly what the app runs.
import PackageDescription

let package = Package(
    name: "NisabaREPL",
    platforms: [.macOS(.v14)],
    dependencies: [
        .package(name: "NisabaDB", path: "../swift")
    ],
    targets: [
        .target(
            name: "NisabaREPLKit",
            dependencies: [
                .product(name: "NisabaDB", package: "NisabaDB")
            ]
        ),
        .executableTarget(
            name: "NisabaREPL",
            dependencies: ["NisabaREPLKit"]
        ),
        .testTarget(
            name: "NisabaREPLKitTests",
            dependencies: ["NisabaREPLKit"]
        )
    ]
)
