//
//  CBCentralManagerDelegateProxy.swift
//  LittleBlueTooth
//
//  Created by Andrea Finollo on 10/06/2020.
//  Copyright © 2020 Andrea Finollo. All rights reserved.
//

import Foundation
import Combine
import os.log
#if TEST
import CoreBluetoothMock
#else
import CoreBluetooth
#endif

public enum ConnectionEvent {
    /// Peripheral is connected but not ready to receive command
    case connected(CBPeripheral)
    /// Peripheral is connected after a disconnection automatically but not ready to receive command
    case autoConnected(CBPeripheral)
    /// Peripheral is ready to receive command
    case ready(CBPeripheral)
    /// Peripheral is not ready probaly due to some unexpected disconnection
    case notReady(CBPeripheral, error: LittleBluetoothError?)
    /// Process of connection has failed
    case connectionFailed(CBPeripheral, error: LittleBluetoothError?)
    /// Peripheral has been disconnected, if it was unexpected a `LittleBluetoothError` is returned
    case disconnected(CBPeripheral, error: LittleBluetoothError?)
}

/// An enumeration representing the state of the bluetooth stack of the device
public enum BluetoothState {
    /// Unknown, probably transient
    case unknown
    /// Bluetooth is resetting
    case resetting
    /// Bluetooth unsupported on this device
    case unsupported
    /// Bluetooth is unauthorized
    case unauthorized
    /// Bluetooth is powered off
    case poweredOff
    /// Bluetooth is powered on
    case poweredOn

    init(_ state: CBManagerState) {
        switch state {
        case .unknown:
            self = .unknown
        case .resetting:
            self = .resetting
        case .unsupported:
            self = .unsupported
        case .unauthorized:
            self = .unauthorized
        case .poweredOff:
            self = .poweredOff
        case .poweredOn:
            self = .poweredOn
        @unknown default:
            self = .unknown
        }
    }
}

class CBCentralManagerDelegateProxy: NSObject {

    /// Publisher containing discovered peripherals
    let centralDiscoveriesPublisher = PassthroughSubject<PeripheralDiscovery, Never>()
    /// Connection event publisher
    let connectionEventPublisher = PassthroughSubject<ConnectionEvent, Never>()
    /// Central state publisher
    let _centralStatePublisher = CurrentValueSubject<BluetoothState, Never>(.unknown)

    var centralStatePublisher: AnyPublisher<BluetoothState, Never> {
        _centralStatePublisher.eraseToAnyPublisher()
    }

    lazy var willRestoreStatePublisher: AnyPublisher<CentralRestorer, Never> = {
        _willRestoreStatePublisher.shareReplay(1).eraseToAnyPublisher()
    }()

    let _willRestoreStatePublisher = PassthroughSubject<CentralRestorer, Never>()

    var isAutoconnectionActive = false
    var logHandler: LBTLogHandler?
    var stateRestorationCancellable: AnyCancellable!

    override init() {
        super.init()
        self.stateRestorationCancellable = willRestoreStatePublisher.sink { _ in }
    }

}

extension CBCentralManagerDelegateProxy: CBCentralManagerDelegate {
    func centralManagerDidUpdateState(_ central: CBCentralManager) {
        logHandler?("Central state: \(BluetoothState(central.state))", .debug, .connection)
        _centralStatePublisher.send(BluetoothState(central.state))
    }

    /// Scan
    func centralManager(_ central: CBCentralManager, didDiscover peripheral: CBPeripheral, advertisementData: [String : Any], rssi RSSI: NSNumber) {
        logHandler?("Discovered \(peripheral.name ?? peripheral.identifier.uuidString)", .debug, .scan)
        let peripheraldiscovery = PeripheralDiscovery(peripheral, advertisement: advertisementData, rssi: RSSI)
        centralDiscoveriesPublisher.send(peripheraldiscovery)
    }

    /// Monitoring connection
    func centralManager(_ central: CBCentralManager, didConnect: CBPeripheral) {
        logHandler?("didConnect \(didConnect.name ?? didConnect.identifier.uuidString)", .info, .connection)
        if isAutoconnectionActive {
            isAutoconnectionActive = false
            let event = ConnectionEvent.autoConnected(didConnect)
            connectionEventPublisher.send(event)
        } else {
            let event = ConnectionEvent.connected(didConnect)
            connectionEventPublisher.send(event)
        }
    }

    func centralManager(_ central: CBCentralManager, didDisconnectPeripheral peripheral: CBPeripheral, timestamp: CFAbsoluteTime, isReconnecting: Bool, error: Error?) {
        let errorDesc: String
        if let error = error {
            let ns = error as NSError
            errorDesc = "\(ns.domain) code=\(ns.code) (\(ns.localizedDescription))"
        } else {
            errorDesc = "none"
        }
        let level: LBTLogLevel = (error != nil && !isReconnecting) ? .warning : .info
        logHandler?("didDisconnect \(peripheral.identifier), isReconnecting: \(isReconnecting), error: \(errorDesc)", level, .connection)
        isAutoconnectionActive = false
        var lttlError: LittleBluetoothError?
        if let error = error {
            lttlError = .peripheralDisconnected(PeripheralIdentifier(peripheral: peripheral), error)
        }
        let event = ConnectionEvent.disconnected(peripheral, error: lttlError)
        connectionEventPublisher.send(event)
    }

    func centralManager(_ central: CBCentralManager, didFailToConnect: CBPeripheral, error: Error?) {
        logHandler?("didFailToConnect \(didFailToConnect.identifier), error: \(error?.localizedDescription ?? "none")", .warning, .connection)
        isAutoconnectionActive = false
        var lttlError: LittleBluetoothError?
        if let error = error {
            lttlError = .couldNotConnectToPeripheral(PeripheralIdentifier(peripheral: didFailToConnect), error)
        }
        let event = ConnectionEvent.connectionFailed(didFailToConnect, error: lttlError)
        connectionEventPublisher.send(event)
    }

    func centralManager(_ central: CBCentralManager, willRestoreState dict: [String : Any]) {
        logHandler?("willRestoreState", .info, .restore)
        _willRestoreStatePublisher.send(CentralRestorer(centralManager: central, restoredInfo: dict))
    }

    #if !os(macOS)
    func centralManager(_ central: CBCentralManager, connectionEventDidOccur event: CBConnectionEvent, for peripheral: CBPeripheral) {}
    #endif

}
