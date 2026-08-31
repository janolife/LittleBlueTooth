//
//  LittleBlueToothTestHooks.swift
//  LittleBlueTooth
//
//  Created By Casey Robinson on 2026-08-26
//

#if TEST
import Foundation

/// Test-only seam for the CoreBluetoothMock harness, where
/// CBMPeripheralMock.openL2CAPChannel is unimplemented (fatalError upstream).
/// The result is delivered asynchronously on the main queue through the normal
/// didOpen proxy path. Not thread-safe: install/uninstall only between tests,
/// with no open in flight.
public enum LittleBlueToothTestHooks {
    nonisolated(unsafe) public static var l2capChannelProvider: ((CBL2CAPPSM) -> Result<CBL2CAPChannel, Error>)?
}

/// Swift 6 mode: the proxy is main-queue-confined in practice (CBM delivers on
/// main); the box only carries it across the dispatch boundary.
final class UncheckedSendableBox<T>: @unchecked Sendable {
    let value: T
    init(_ value: T) { self.value = value }
}
#endif
