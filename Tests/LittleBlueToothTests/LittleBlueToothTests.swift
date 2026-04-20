//
//  LittleBlueToothTests.swift
//  LittleBlueToothTests
//
//  Created by Andrea Finollo on 10/06/2020.
//  Copyright © 2020 Andrea Finollo. All rights reserved.
//

import XCTest
import CoreBluetoothMock
import Combine
@testable import LittleBlueToothForTest

@MainActor
class LittleBlueToothTests: XCTestCase {
    var littleBT: LittleBlueTooth!
    var disposeBag: Set<AnyCancellable> = []
    nonisolated(unsafe) static var testInitialized: Bool = false
    
    override func setUp() async throws {
        try await super.setUp()
        if !Self.testInitialized {
            CBMCentralManagerMock.simulatePeripherals([blinky, blinkyWOR])
            Self.testInitialized = true
        }
        CBMCentralManagerMock.simulateInitialState(.poweredOn)
    }
}
