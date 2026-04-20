// swift-tools-version: 6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let swift6: [SwiftSetting] = [.swiftLanguageMode(.v6)]

let package = Package(
    name: "LittleBlueTooth",
    platforms: [
        // Add support for all platforms starting from a specific version.
        .macOS(.v10_15),
        .iOS(.v13),
        .watchOS(.v6),
        .tvOS(.v13)
    ],
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "LittleBlueTooth",
            targets: ["LittleBlueTooth"]),
        .library(
            name: "LittleBlueToothForTest",
            targets: ["LittleBlueToothForTest"])
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        .package(url: "https://github.com/NordicSemiconductor/IOS-CoreBluetooth-Mock.git",
                 .upToNextMinor(from: "1.0.4")),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "LittleBlueTooth",
            dependencies: [],
            exclude: ["Info.plist"],
            swiftSettings: swift6
        ),
        .target(
            name: "LittleBlueToothForTest",
            dependencies: [.product(name: "CoreBluetoothMock", package: "IOS-CoreBluetooth-Mock")],
            exclude: ["Info.plist"],
            swiftSettings: swift6 + [.define("TEST")]
        ),
        .testTarget(
            name: "LittleBlueToothTests",
            dependencies: ["LittleBlueToothForTest", .product(name: "CoreBluetoothMock", package: "IOS-CoreBluetooth-Mock")],
            exclude: ["Info.plist"],
            swiftSettings: swift6
        )
    ]
)
