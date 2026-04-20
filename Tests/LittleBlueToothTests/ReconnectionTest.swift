//
//  ReconnectionTest.swift
//  LittleBlueToothTests
//
//  Tests for the connection lifecycle rewrite.
//  Based on Apple's CoreBluetooth best practices:
//  - Autoconnection calls cbCentral.connect() directly in disconnect handler
//  - Connection request stays pending forever (no timeout, no retry loop)
//  - didConnect fires when peripheral becomes available
//

import XCTest
import CoreBluetoothMock
import Combine
@testable import LittleBlueToothForTest

/// Thread-safe log collector for unstructured string logs
final class LogCollector: @unchecked Sendable {
    private let lock = NSLock()
    private var _messages = [String]()

    var messages: [String] {
        lock.lock()
        defer { lock.unlock() }
        return _messages
    }

    func append(_ message: String) {
        lock.lock()
        _messages.append(message)
        lock.unlock()
    }

    func contains(_ substring: String) -> Bool {
        messages.contains(where: { $0.contains(substring) })
    }
}

/// Thread-safe collector for structured logs
final class StructuredLogCollector: @unchecked Sendable {
    struct Entry {
        let message: String
        let level: LBTLogLevel
        let category: LBTLogCategory
    }

    private let lock = NSLock()
    private var _messages = [Entry]()

    var messages: [Entry] {
        lock.lock()
        defer { lock.unlock() }
        return _messages
    }

    func append(_ message: String, _ level: LBTLogLevel, _ category: LBTLogCategory) {
        lock.lock()
        _messages.append(Entry(message: message, level: level, category: category))
        lock.unlock()
    }
}

class ReconnectionTest: LittleBlueToothTests {

    override func setUpWithError() throws {
        try MainActor.assumeIsolated {
            try super.setUpWithError()
            // Reset the mock's global state between tests. Autoconnection leaves
            // pending cbCentral.connect() requests alive on the previous test's
            // CBMCentralManager; without a full reset those linger and interfere
            // with the next test's discovery/connection flow.
            CBMCentralManagerMock.tearDownSimulation()
            CBMCentralManagerMock.simulatePeripherals([blinky, blinkyWOR])
            CBMCentralManagerMock.simulateInitialState(.poweredOn)

            // Ensure clean state — blinky out of range, not connected
            blinky.simulateProximityChange(.outOfRange)
            var configuration = LittleBluetoothConfiguration()
            configuration.isLogEnabled = true
            littleBT = LittleBlueTooth(with: configuration)
        }
    }

    override func tearDownWithError() throws {
        try MainActor.assumeIsolated {
            littleBT.autoconnectionHandler = nil
            littleBT.disconnect()
            disposeBag.removeAll()
            blinky.simulateProximityChange(.outOfRange)
            try super.tearDownWithError()
        }
    }

    /// Helper: connect to blinky and wait for ready
    private func connectBlinky() -> XCTestExpectation {
        blinky.simulateProximityChange(.immediate)
        let connected = expectation(description: "Connected to blinky")
        littleBT.startDiscovery(withServices: nil)
            .flatMap { discovery in
                self.littleBT.connect(to: discovery)
            }
            .sink(receiveCompletion: { _ in }, receiveValue: { _ in
                connected.fulfill()
            })
            .store(in: &disposeBag)
        return connected
    }

    // MARK: - Autoconnection fires on disconnect

    /// When a peripheral disconnects unexpectedly and autoconnectionHandler returns true,
    /// the library should call cbCentral.connect() directly. When the peripheral
    /// becomes available again, didConnect should fire and the connection should
    /// be re-established without any retry loops.
    func testAutoconnectionReconnectsAfterDisconnect() {
        disposeBag.removeAll()
        blinky.simulateProximityChange(.immediate)

        let reconnectedExpectation = expectation(description: "Peripheral reconnected after disconnect")
        reconnectedExpectation.assertForOverFulfill = false
        var events = [ConnectionEvent]()

        littleBT.autoconnectionHandler = { _, _ in true }

        // Subscribe to connection events to track the lifecycle
        littleBT.connectionEventPublisher
            .sink { event in
                events.append(event)
                // After autoConnected + ready, we're reconnected
                if case .ready(_) = event, events.count > 3 {
                    reconnectedExpectation.fulfill()
                }
            }
            .store(in: &disposeBag)

        // Connect, then simulate disconnect after 2s, then bring back after 2 more
        littleBT.startDiscovery(withServices: nil)
            .flatMap { discovery in
                self.littleBT.connect(to: discovery)
            }
            .sink(receiveCompletion: { _ in }, receiveValue: { _ in
                // Connected. Now simulate disconnect.
                DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                    blinky.simulateDisconnection()
                    // Bring back in range after 2s — CB's pending connect should pick it up
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                        blinky.simulateProximityChange(.immediate)
                    }
                }
            })
            .store(in: &disposeBag)

        waitForExpectations(timeout: 15)

        // Should have: connected, ready, disconnected, autoConnected, ready
        XCTAssertGreaterThanOrEqual(events.count, 5)
        littleBT.autoconnectionHandler = nil
        littleBT.disconnect()
    }

    // MARK: - Peripheral state publisher survives reconnect

    /// External subscribers to peripheralStatePublisher should continue receiving
    /// state updates after a disconnect/reconnect cycle. The publisher must be
    /// stable across reconnections.
    func testPeripheralStatePublisherSurvivesReconnect() {
        disposeBag.removeAll()
        blinky.simulateProximityChange(.immediate)

        let stateAfterReconnect = expectation(description: "State publisher fires after reconnect")
        var states = [PeripheralState]()

        littleBT.autoconnectionHandler = { _, _ in true }

        // Subscribe to peripheral state BEFORE first connection
        littleBT.peripheralStatePublisher
            .sink { state in
                states.append(state)
                // After reconnection we should see .connected again
                if state == .connected && states.contains(.disconnected) {
                    stateAfterReconnect.fulfill()
                }
            }
            .store(in: &disposeBag)

        littleBT.startDiscovery(withServices: nil)
            .flatMap { discovery in
                self.littleBT.connect(to: discovery)
            }
            .sink(receiveCompletion: { _ in }, receiveValue: { _ in
                DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                    blinky.simulateDisconnection()
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                        blinky.simulateProximityChange(.immediate)
                    }
                }
            })
            .store(in: &disposeBag)

        waitForExpectations(timeout: 15)

        // Must have seen disconnected AND connected after it
        XCTAssertTrue(states.contains(.disconnected))
        XCTAssertTrue(states.contains(.connected))
        let disconnectIndex = states.lastIndex(of: .disconnected)!
        let reconnectIndex = states.lastIndex(of: .connected)!
        XCTAssertGreaterThan(reconnectIndex, disconnectIndex, "Connected state should come after disconnected")

        littleBT.autoconnectionHandler = nil
        littleBT.disconnect()
    }

    // MARK: - Autoconnection options are passed through

    /// When autoconnectionOptions are configured, they should be passed to
    /// cbCentral.connect() during autoconnection.
    func testAutoconnectionPassesOptions() {
        disposeBag.removeAll()
        blinky.simulateProximityChange(.immediate)

        let reconnectedExpectation = expectation(description: "Reconnected with options")
        reconnectedExpectation.assertForOverFulfill = false
        let logs = LogCollector()

        var config = LittleBluetoothConfiguration()
        config.isLogEnabled = true
        config.autoconnectionHandler = { _, _ in true }
        config.autoconnectionOptions = [
            CBConnectPeripheralOptionNotifyOnConnectionKey: true as NSNumber,
            CBConnectPeripheralOptionNotifyOnDisconnectionKey: true as NSNumber,
        ]
        config.logHandler = { message, level, category in
            logs.append(message)
        }
        littleBT = LittleBlueTooth(with: config)

        littleBT.connectionEventPublisher
            .sink { event in
                if case .ready(_) = event {
                    if logs.contains("kCBConnectOptionNotifyOnConnection") {
                        reconnectedExpectation.fulfill()
                    }
                }
            }
            .store(in: &disposeBag)

        littleBT.startDiscovery(withServices: nil)
            .flatMap { discovery in
                self.littleBT.connect(to: discovery)
            }
            .sink(receiveCompletion: { _ in }, receiveValue: { _ in
                DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                    blinky.simulateDisconnection()
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                        blinky.simulateProximityChange(.immediate)
                    }
                }
            })
            .store(in: &disposeBag)

        waitForExpectations(timeout: 15)

        XCTAssertTrue(logs.contains("kCBConnectOptionNotifyOnConnection"),
                      "Autoconnection options should be logged when connect is called")
        littleBT.disconnect()
    }

    // MARK: - Direct connect on disconnect (no publisher chain)

    /// After disconnect, autoconnection should NOT produce connectionFailed events.
    /// It should call cbCentral.connect() directly and wait silently until
    /// the peripheral becomes available. The connect() publisher chain is only
    /// for user-initiated connections.
    func testAutoconnectionDoesNotProduceConnectionFailedEvents() {
        littleBT.autoconnectionHandler = { _, _ in true }

        // Phase 1: Connect
        let connectedExp = connectBlinky()
        wait(for: [connectedExp], timeout: 10)

        // Phase 2: Track events during reconnection
        let reconnectedExp = expectation(description: "Reconnected")
        reconnectedExp.assertForOverFulfill = false
        var sawConnectionFailed = false

        littleBT.connectionEventPublisher
            .sink { event in
                if case .connectionFailed(_, _) = event { sawConnectionFailed = true }
                if case .ready(_) = event { reconnectedExp.fulfill() }
            }
            .store(in: &disposeBag)

        // Disconnect, then bring back
        blinky.simulateDisconnection()
        DispatchQueue.main.asyncAfter(deadline: .now() + 3.0) {
            blinky.simulateProximityChange(.immediate)
        }

        wait(for: [reconnectedExp], timeout: 15)

        // The autoconnection path should NOT produce connectionFailed events.
        // If it does, it means the publisher chain is being used instead of
        // a direct cbCentral.connect() call.
        XCTAssertFalse(sawConnectionFailed,
                       "Autoconnection should use direct connect, not publisher chain that can produce connectionFailed")
    }

    // MARK: - Basic state publisher works

    /// Verify the peripheralStatePublisher receives states during a normal connection.
    func testPeripheralStatePublisherReceivesInitialConnection() {
        disposeBag.removeAll()
        blinky.simulateProximityChange(.immediate)

        let gotConnected = expectation(description: "State publisher emits connected")
        var states = [PeripheralState]()

        littleBT.peripheralStatePublisher
            .sink { state in
                print("TEST: got state \(state)")
                states.append(state)
                if state == .connected {
                    gotConnected.fulfill()
                }
            }
            .store(in: &disposeBag)

        littleBT.startDiscovery(withServices: nil)
            .flatMap { discovery in
                self.littleBT.connect(to: discovery)
            }
            .sink(receiveCompletion: { _ in }, receiveValue: { _ in })
            .store(in: &disposeBag)

        waitForExpectations(timeout: 10)
        XCTAssertTrue(states.contains(.connected))
    }

    // MARK: - Structured logging

    /// The logHandler should receive structured log messages with level and category.
    func testStructuredLogHandlerReceivesConnectionEvents() {
        disposeBag.removeAll()
        blinky.simulateProximityChange(.immediate)

        let disconnectLogged = expectation(description: "Disconnect logged via handler")
        let logs = StructuredLogCollector()

        var config = LittleBluetoothConfiguration()
        config.logHandler = { message, level, category in
            logs.append(message, level, category)
            if category == .connection && message.hasPrefix("Disconnected:") {
                disconnectLogged.fulfill()
            }
        }
        littleBT = LittleBlueTooth(with: config)

        littleBT.startDiscovery(withServices: nil)
            .flatMap { discovery in
                self.littleBT.connect(to: discovery)
            }
            .sink(receiveCompletion: { _ in }, receiveValue: { _ in
                DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                    blinky.simulateDisconnection()
                }
            })
            .store(in: &disposeBag)

        waitForExpectations(timeout: 15)

        // Should have connection-category logs
        let connectionLogs = logs.messages.filter { $0.category == .connection }
        XCTAssertFalse(connectionLogs.isEmpty, "Should have connection category logs")

        // Disconnect should be at .info level
        let disconnectLogs = connectionLogs.filter { $0.message.hasPrefix("Disconnected:") }
        XCTAssertFalse(disconnectLogs.isEmpty, "Should log disconnect")
        XCTAssertEqual(disconnectLogs.first?.level, .info)
    }

    /// Verify proxy-level delegate events flow through structured logHandler
    func testProxyLogsFlowThroughLogHandler() {
        let logs = StructuredLogCollector()
        var config = LittleBluetoothConfiguration()
        config.logHandler = { message, level, category in
            logs.append(message, level, category)
        }
        littleBT = LittleBlueTooth(with: config)

        let connectedExp = connectBlinky()
        wait(for: [connectedExp], timeout: 10)

        // Proxy should have logged didConnect
        let connectionProxyLogs = logs.messages.filter {
            $0.category == .connection && $0.message.contains("didConnect")
        }
        XCTAssertFalse(connectionProxyLogs.isEmpty, "Proxy didConnect should flow through logHandler")
    }

    /// Verify GATT operations produce gatt-category logs
    func testStructuredLogHandlerReceivesGATTLogs() {
        let logs = StructuredLogCollector()
        var config = LittleBluetoothConfiguration()
        config.logHandler = { message, level, category in
            logs.append(message, level, category)
        }
        littleBT = LittleBlueTooth(with: config)

        let readDone = connectBlinky()
        wait(for: [readDone], timeout: 10)

        let readExpectation = expectation(description: "Read complete")
        let characteristic = LittleBlueToothCharacteristic(
            characteristic: CBMUUID.ledCharacteristic.uuidString,
            for: CBMUUID.nordicBlinkyService.uuidString,
            properties: [.read, .write]
        )
        littleBT.read(from: characteristic)
            .sink(receiveCompletion: { _ in }, receiveValue: { (state: LedState) in
                readExpectation.fulfill()
            })
            .store(in: &disposeBag)

        wait(for: [readExpectation], timeout: 10)

        let gattLogs = logs.messages.filter { $0.category == .gatt }
        XCTAssertFalse(gattLogs.isEmpty, "Read operation should produce gatt-category logs")
    }
}
