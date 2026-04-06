//
//  CBPeripheralProxy.swift
//  LittleBlueTooth
//
//  Created by Andrea Finollo on 10/06/2020.
//  Copyright © 2020 Andrea Finollo. All rights reserved.
//

import Foundation
import Combine
#if TEST
import CoreBluetoothMock
#else
import CoreBluetooth
#endif

final class CBPeripheralDelegateProxy: NSObject {

    let peripheralChangesPublisher = PassthroughSubject<PeripheralChanges, Never>()
    let peripheralRSSIPublisher = PassthroughSubject<(Int, LittleBluetoothError?), Never>()

    lazy var peripheralDiscoveredServicesPublisher = { _peripheralDiscoveredServicesPublisher.share().eraseToAnyPublisher()
    }()
    let _peripheralDiscoveredServicesPublisher = PassthroughSubject<([CBService]?, LittleBluetoothError?), Never>()

    lazy var peripheralDiscoveredIncludedServicesPublisher = { _peripheralDiscoveredIncludedServicesPublisher.share().eraseToAnyPublisher()
    }()
    let _peripheralDiscoveredIncludedServicesPublisher = PassthroughSubject<(CBService, Error?), Never>()

    lazy var peripheralDiscoveredCharacteristicsForServicePublisher = { _peripheralDiscoveredCharacteristicsForServicePublisher.share().eraseToAnyPublisher()
    }()
    let _peripheralDiscoveredCharacteristicsForServicePublisher = PassthroughSubject<(CBService, LittleBluetoothError?), Never>()

    lazy var peripheralUpdatedNotificationStateForCharacteristicPublisher = { _peripheralUpdatedNotificationStateForCharacteristicPublisher.share().eraseToAnyPublisher()
    }()
    let _peripheralUpdatedNotificationStateForCharacteristicPublisher =
        PassthroughSubject<(CBCharacteristic, LittleBluetoothError?), Never>()

    let peripheralUpdatedValueForCharacteristicPublisher = PassthroughSubject<(CBCharacteristic, LittleBluetoothError?), Never>()
    let peripheralUpdatedValueForNotifyCharacteristicPublisher = PassthroughSubject<(CBCharacteristic, LittleBluetoothError?), Never>()
    let peripheralWrittenValueForCharacteristicPublisher = PassthroughSubject<(CBCharacteristic, LittleBluetoothError?), Never>()
    let peripheralIsReadyToSendWriteWithoutResponse = PassthroughSubject<Void, Never>()

    let peripheralDiscoveredDescriptorsForCharacteristicPublisher =
        PassthroughSubject<(CBCharacteristic, LittleBluetoothError?), Never>()
    let peripheralUpdatedValueForDescriptor = PassthroughSubject<(CBDescriptor, LittleBluetoothError?), Never>()
    let peripheralWrittenValueForDescriptor = PassthroughSubject<(CBDescriptor, LittleBluetoothError?), Never>()

    let peripheralOpenedL2CAPChannelPublisher = PassthroughSubject<(CBL2CAPChannel?, LittleBluetoothError?), Never>()

    var logHandler: LBTLogHandler?
}

extension CBPeripheralDelegateProxy: CBPeripheralDelegate {

    func peripheralIsReady(toSendWriteWithoutResponse peripheral: CBPeripheral) {
        logHandler?("Ready to send write without response", .trace, .gatt)
        peripheralIsReadyToSendWriteWithoutResponse.send()
    }

    func peripheralDidUpdateName(_ peripheral: CBPeripheral) {
        logHandler?("Name updated: \(peripheral.name ?? "nil")", .debug, .gatt)
        peripheralChangesPublisher.send(.name(peripheral.name))
    }

    func peripheral(_ peripheral: CBPeripheral, didModifyServices invalidatedServices: [CBService]) {
        logHandler?("Services modified: \(invalidatedServices)", .info, .gatt)
        peripheralChangesPublisher.send(.invalidatedServices(invalidatedServices))
    }

    func peripheral(_ peripheral: CBPeripheral, didReadRSSI RSSI: NSNumber, error: Error?) {
        if let error = error {
            peripheralRSSIPublisher.send((RSSI.intValue, .couldNotReadRSSI(error)))
        } else {
            peripheralRSSIPublisher.send((RSSI.intValue, nil))
        }
    }

    func peripheral(_ peripheral: CBPeripheral, didDiscoverServices error: Error?) {
        logHandler?("Discovered services, error: \(error?.localizedDescription ?? "none")", .debug, .gatt)
        if let error = error {
            _peripheralDiscoveredServicesPublisher.send((nil, .serviceNotFound(error)))
        } else {
            _peripheralDiscoveredServicesPublisher.send((peripheral.services, nil))
        }
    }

    func peripheral(_ peripheral: CBPeripheral, didDiscoverIncludedServicesFor service: CBService, error: Error?) {
        logHandler?("Discovered included services for \(service.uuid), error: \(error?.localizedDescription ?? "none")", .debug, .gatt)
        if let error = error {
            _peripheralDiscoveredIncludedServicesPublisher.send((service, error))
        } else {
            _peripheralDiscoveredIncludedServicesPublisher.send((service, nil))
        }
    }

    func peripheral(_ peripheral: CBPeripheral, didDiscoverCharacteristicsFor service: CBService, error: Error?) {
        logHandler?("Discovered characteristics for \(service.uuid), error: \(error?.localizedDescription ?? "none")", .debug, .gatt)
        if let error = error {
            _peripheralDiscoveredCharacteristicsForServicePublisher.send((service, .characteristicNotFound(error)))
        } else {
            _peripheralDiscoveredCharacteristicsForServicePublisher.send((service, nil))
        }
    }

    func peripheral(_ peripheral: CBPeripheral, didUpdateValueFor characteristic: CBCharacteristic, error: Error?) {
        logHandler?("Value updated for \(characteristic.uuid), error: \(error?.localizedDescription ?? "none")", .trace, .gatt)
        if let error = error {
            peripheralUpdatedValueForCharacteristicPublisher.send((characteristic, .couldNotReadFromCharacteristic(characteristic: characteristic.uuid, error: error)))
        } else {
            if !characteristic.isNotifying {
                peripheralUpdatedValueForCharacteristicPublisher.send((characteristic, nil))
            } else {
                peripheralUpdatedValueForNotifyCharacteristicPublisher.send((characteristic, nil))
            }
        }
    }

    func peripheral(_ peripheral: CBPeripheral, didWriteValueFor characteristic: CBCharacteristic, error: Error?) {
        logHandler?("Wrote value for \(characteristic.uuid), error: \(error?.localizedDescription ?? "none")", .debug, .gatt)
        if let error = error {
            peripheralWrittenValueForCharacteristicPublisher.send((characteristic, .couldNotWriteFromCharacteristic(characteristic: characteristic.uuid, error: error)))
        } else {
            peripheralWrittenValueForCharacteristicPublisher.send((characteristic, nil))
        }
    }

    func peripheral(_ peripheral: CBPeripheral, didUpdateNotificationStateFor characteristic: CBCharacteristic, error: Error?) {
        logHandler?("Notification state for \(characteristic.uuid): \(characteristic.isNotifying), error: \(error?.localizedDescription ?? "none")", .debug, .gatt)
        if let error = error {
            _peripheralUpdatedNotificationStateForCharacteristicPublisher.send((characteristic, .couldNotUpdateListenState(characteristic: characteristic.uuid, error: error)))
        } else {
            _peripheralUpdatedNotificationStateForCharacteristicPublisher.send((characteristic, nil))
        }
    }

    func peripheral(_ peripheral: CBPeripheral, didOpen channel: CBL2CAPChannel?, error: Error?) {
        logHandler?("L2CAP channel opened, error: \(error?.localizedDescription ?? "none")", .info, .l2cap)
        if let error = error {
            peripheralOpenedL2CAPChannelPublisher.send((nil, .couldNotOpenL2CAPChannel(error: error)))
        } else {
            peripheralOpenedL2CAPChannelPublisher.send((channel, nil))
        }
    }
}
