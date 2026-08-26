//
//  Peripheral.swift
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


/// An enumeration that represent the changes in peripheral services or name
public enum PeripheralChanges {
    /// The name has been changed
    case name(String?)
    /// Some services have been invalidated
    case invalidatedServices([CBService])
}

/// The state of the peripheral
public enum PeripheralState: Sendable {
    /// Peripheral is disconnected
    case disconnected
    /// Peripheral is connecting
    case connecting
    /// Peripheral is connected
    case connected
    /// Peripheral is disconnecting
    case disconnecting
    /// The peripheral state is not know, could be transitory
    case unknown
    
    /// Initilize a Peripheral state using a `CBPeripheralState`
    init(state: CBPeripheralState) {
        switch state {
        case .disconnected:
            self = .disconnected
        case .connected:
            self = .connected
        case .disconnecting:
            self = .disconnecting
        case .connecting:
            self = .connecting
        default:
            self = .unknown
        }
    }
}

/// It represents a peripheral along with its properties
// @unchecked Sendable is a runtime contract, not a compile-time proof. This
// type wraps a `CBPeripheral` whose delegate callbacks are dispatched on the
// owning `CBCentralManager`'s queue. The internal `SubscriptionBag` is
// lock-guarded, and `cbPeripheral` is set once at init and never reassigned.
//
// Publicly mutable properties (`skipServiceCache`, `isLogEnabled`, `logHandler`)
// are not isolated. Callers are expected — but not forced — to assign them
// from a single thread, typically during setup. Concurrent assignment from
// multiple threads is undefined behavior.
public final class Peripheral: Identifiable, @unchecked Sendable {
    /// An identifier for the peripheral it is the same as the wrapped `CBPeripheral`
    public var id: UUID {
        cbPeripheral.identifier
    }
    /// The name of the peripheral it is the same as the wrapped `CBPeripheral`
    public var name: String? {
        cbPeripheral.name
    }
    /// The state of the peripheral it is the same as the wrapped `CBPeripheral`
    public var state: PeripheralState {
        PeripheralState(state: cbPeripheral.state)
    }
    
    /// The wrapped `CBPeripheral`
    public let cbPeripheral: CBPeripheral

    /// When true, `getService` bypasses the cached services check and always
    /// performs fresh BLE discovery. Set this after state restoration where
    /// cached services may have stale/empty characteristic values.
    public var skipServiceCache: Bool = false

    /// Logging on the peripheral can be disable or enabled acting of that property
    var isLogEnabled: Bool {
        get {
            return _isLogEnabled
        }
        set {
            _isLogEnabled = newValue
        }
    }

    /// Structured log handler, forwarded to the peripheral proxy
    var logHandler: LBTLogHandler? {
        didSet {
            peripheralProxy.logHandler = logHandler
        }
    }
    
    /// The publisher that listen to changes in peripheral name or services
    lazy var changesPublisher: AnyPublisher<PeripheralChanges, Never> =
               peripheralProxy.peripheralChangesPublisher
               .share()
               .eraseToAnyPublisher()
    /// The publisher that listen to characteristic notifications
    lazy var listenPublisher: AnyPublisher<CBCharacteristic, LittleBluetoothError> =
            peripheralProxy.peripheralUpdatedValueForNotifyCharacteristicPublisher
            .tryMap { (value) -> CBCharacteristic in
                switch value {
                case let (_, error?):
                    throw error
                case let (charact, _):
                    return charact
                }
            }
            .mapError {$0 as! LittleBluetoothError}
            .share()
            .eraseToAnyPublisher()
    
    let peripheralStatePublisher: AnyPublisher<PeripheralState, Never>
    
    private let peripheralProxy = CBPeripheralDelegateProxy()
    private var _isLogEnabled: Bool = false
    private let disposeBag = SubscriptionBag()

    /// Re-set the CBPeripheral delegate to this Peripheral's proxy.
    /// Call after ensuring all old Peripheral references are released,
    /// to prevent a deferred ARC deallocation from clearing the weak delegate.
    func reassertDelegate() {
        cbPeripheral.delegate = peripheralProxy
    }

    /// Initialize a `Peripheral` using a `CBperipheral`
    /// It also attach the publisher to monitor the state of the peripheral
    init(_ peripheral: CBPeripheral) {
        self.cbPeripheral = peripheral
        self.cbPeripheral.delegate = self.peripheralProxy
        
        #if !TEST
        self.peripheralStatePublisher = self.cbPeripheral.publisher(for: \.state)
            .map{ (state) -> PeripheralState in
                PeripheralState(state: state)
            }
            .share()
            .eraseToAnyPublisher()
        // Using a timer to poll peripheral state for test to simulate KVO
        #else
        self.peripheralStatePublisher = Timer.publish(every: 0.2, on: .main, in: .common)
        .autoconnect()
        .map {_ in
            PeripheralState(state: peripheral.state)
        }
        .eraseToAnyPublisher()
        #endif
    }

    deinit {
        if cbPeripheral.delegate === peripheralProxy {
            cbPeripheral.delegate = nil
        }
    }

    private func removeAndCancelSubscriber(for key: UUID) {
        disposeBag.remove(key)
    }
    
    func getService(serviceUUID: CBUUID?) -> AnyPublisher<[CBService]?, LittleBluetoothError> {
        if !skipServiceCache,
           let serviceUUID,
           let services = self.cbPeripheral.services,
           services.contains(where: { $0.uuid == serviceUUID }) {
            return Result<[CBService]?, LittleBluetoothError>.Publisher(.success(services)).eraseToAnyPublisher()
        } else {
            let futKey = UUID()
            let services = Deferred {
                Future<[CBService]?, LittleBluetoothError> { [unowned self, futKey] promise in
                    self.peripheralProxy.peripheralDiscoveredServicesPublisher
                        .tryMap { (value) -> [CBService]? in
                            switch value {
                            case let (_, error?):
                                throw error
                            case let (services?, _) where services.map{$0.uuid}.contains(serviceUUID) && serviceUUID != nil:
                                return services
                            case let (services?, _) where serviceUUID == nil:
                                return services
                            case (_, .none):
                                throw LittleBluetoothError.serviceNotFound(nil)
                            }
                        }
                        .mapError {$0 as! LittleBluetoothError}
                        .sink { (completion) in
                            switch completion {
                            case .finished:
                                break
                            case .failure(let error):
                                promise(.failure(error))
                                self.removeAndCancelSubscriber(for: futKey)
                            }
                        } receiveValue: { (service) in
                            promise(.success(service))
                            self.removeAndCancelSubscriber(for: futKey)
                        }
                        .store(in: self.disposeBag, for: futKey)
                }
            }
            .eraseToAnyPublisher()
            defer {
                self.cbPeripheral.discoverServices(serviceUUID != nil ? [serviceUUID!] : nil)
            }
            return services.eraseToAnyPublisher()
        }
    }
    
    fileprivate func getCharateristics(characteristicUUIDs: [CBUUID]?, from service: CBService) -> AnyPublisher<CBService, LittleBluetoothError> {
        // Check if has beed already discovered
        if let characteristics = service.characteristics,
           let characteristicUUIDs,
            characteristics.contains(where: { (charact) -> Bool in
                return characteristicUUIDs.contains(charact.uuid)
        }) {
            return Result<CBService, LittleBluetoothError>.Publisher(.success(service)).eraseToAnyPublisher()
        } else {
            let charact =
                self.peripheralProxy.peripheralDiscoveredCharacteristicsForServicePublisher
                .tryMap { (value) -> CBService in
                    switch value {
                    case let (_, error?):
                        throw error
                    case let (service, _):
                        return service
                    }
                }
                .mapError {$0 as! LittleBluetoothError}
            defer {
                self.cbPeripheral.discoverCharacteristics(characteristicUUIDs, for: service)
            }
            return charact.eraseToAnyPublisher()
        }
    }
    
    func discoverCharacteristics(_ charateristicUUIDs: [CBUUID]?, fromService serviceUUID: CBUUID) -> AnyPublisher<[CBCharacteristic], LittleBluetoothError> {
        // Check if it has already been discovered
        if
            let service = cbPeripheral.services?.first(where: { service in
            service.uuid == serviceUUID
        }),
            let charateristicUUIDs,
            let charact = service.characteristics?.first(where: { characteristic in
                charateristicUUIDs.contains(characteristic.uuid)
        }) {
            return Result<[CBCharacteristic], LittleBluetoothError>.Publisher(.success([charact]))
                .eraseToAnyPublisher()
        }
        
        let futKey = UUID()
        let discovery = Deferred {
            Future<[CBCharacteristic], LittleBluetoothError> { [unowned self, futKey] promise in
                self.getService(serviceUUID: serviceUUID)
                                        .flatMap { services -> AnyPublisher<CBService, LittleBluetoothError> in
                        let service = services!.filter{ $0.uuid == serviceUUID}.first!
                        return self.getCharateristics(characteristicUUIDs: charateristicUUIDs, from: service)
                    }
                                        .filter { (service) -> Bool in
                        if charateristicUUIDs == nil {
                            return true
                        } else if let characteristics = service.characteristics?.map({$0.uuid}),
                           let charateristicUUIDs,
                           Set(characteristics).isSuperset(of: Set(charateristicUUIDs)) {
                            return true
                        } else {
                            return false
                        }
                    }
                    .map { service -> [CBCharacteristic] in
                        if charateristicUUIDs == nil {
                            return service.characteristics ?? []
                        }
                        return service.characteristics!.filter{ characteristic in
                            charateristicUUIDs!.contains(characteristic.uuid)
                        }
                    }
                    .sink { (completion) in
                        switch completion {
                        case .finished:
                            break
                        case .failure(let error):
                            promise(.failure(error))
                            self.removeAndCancelSubscriber(for: futKey)
                        }
                    } receiveValue: { (characteristics) in
                        promise(.success(characteristics))
                        self.removeAndCancelSubscriber(for: futKey)
                    }
                    .store(in: self.disposeBag, for: futKey)
            }
        }.eraseToAnyPublisher()
        
        return discovery
    }
    
    func readRSSI() -> AnyPublisher<Int, LittleBluetoothError> {
        let futKey = UUID()
        let readRSSI = Deferred {
            Future<Int, LittleBluetoothError> { [unowned self, futKey] promise in
                peripheralProxy.peripheralRSSIPublisher
                    .tryMap { (value) -> Int in
                        switch value {
                        case let (_, error?):
                            throw error
                        case let (rssi, _):
                            return rssi
                        }
                    }
                    .mapError {$0 as! LittleBluetoothError}
                    .sink(receiveCompletion: { [unowned self, futKey] (completion) in
                        switch completion {
                        case .finished:
                            break
                        case .failure(let error):
                            promise(.failure(error))
                            self.removeAndCancelSubscriber(for: futKey)
                        }
                    }) { [unowned self, futKey] (readvalue) in
                        promise(.success(readvalue))
                        self.removeAndCancelSubscriber(for: futKey)
                    }
                    .store(in: disposeBag, for: futKey)
            }
        }
        .eraseToAnyPublisher()
        
        defer {
            cbPeripheral.readRSSI()
        }
        return readRSSI
    }
   
    func read(from charateristicUUID: CBUUID, of serviceUUID: CBUUID) -> AnyPublisher<Data?, LittleBluetoothError> {
        let futKey = UUID()
        let read = Deferred {
            Future<Data?, LittleBluetoothError> { [unowned self, futKey] promise in
                discoverCharacteristics([charateristicUUID], fromService: serviceUUID)
                    .flatMap { characteristics -> AnyPublisher<CBCharacteristic, LittleBluetoothError> in
                        let charateristicUUIDFound = characteristics.first(where: { charact in
                            charact.uuid == charateristicUUID
                        })
                        self.cbPeripheral.readValue(for: charateristicUUIDFound!)
                        return self.peripheralProxy.peripheralUpdatedValueForCharacteristicPublisher
                            .filter { [charUUID = charateristicUUID] (value) in
                                value.0.uuid == charUUID
                            }
                            .tryMap { (value) -> CBCharacteristic in
                                switch value {
                                case let (_, error?):
                                    throw error
                                case let (charact, _):
                                    return charact
                                }
                            }
                            .mapError {$0 as! LittleBluetoothError}
                            .eraseToAnyPublisher()
                    }
                    .map { (characteristic) -> Data? in
                        characteristic.value
                    }
                    .sink(receiveCompletion: { [unowned self, futKey] (completion) in
                        switch completion {
                        case .finished:
                            break
                        case .failure(let error):
                            promise(.failure(error))
                            self.removeAndCancelSubscriber(for: futKey)
                        }
                    }) { [unowned self, futKey] (readvalue) in
                        promise(.success(readvalue))
                        self.removeAndCancelSubscriber(for: futKey)
                    }
                    .store(in: disposeBag, for: futKey)
            }
        }
        .eraseToAnyPublisher()
        return read
    }
    
    
    func write(to charateristicUUID: CBUUID, of serviceUUID: CBUUID, data: Data, response: Bool = true) -> AnyPublisher<CBCharacteristic, LittleBluetoothError> {
        
        let futKey = UUID()
        let write = Deferred {
            Future<CBCharacteristic, LittleBluetoothError> { [unowned self, futKey] promise in
                discoverCharacteristics([charateristicUUID], fromService: serviceUUID)
                    .flatMap { characteristics -> AnyPublisher<CBCharacteristic, LittleBluetoothError> in
                        if response {
                            let charateristicUUIDFound = characteristics.first(where: { charact in
                                charact.uuid == charateristicUUID
                            })
                            self.cbPeripheral.writeValue(data, for: charateristicUUIDFound!, type: .withResponse)
                            return self.peripheralProxy.peripheralWrittenValueForCharacteristicPublisher.tryMap { (value) -> CBCharacteristic in
                                switch value {
                                case let (_, error?):
                                    throw error
                                case let (charact, _):
                                    return charact
                                }
                            }
                            .mapError {$0 as! LittleBluetoothError}
                            .eraseToAnyPublisher()
                        } else {
                            let charateristicUUIDFound = characteristics.first(where: { charact in
                                charact.uuid == charateristicUUID
                            })
                            let writeWOResp = self.peripheralProxy.peripheralIsReadyToSendWriteWithoutResponse
                                .map { _ -> Bool in
                                    return true
                                }
                                .prepend([self.cbPeripheral.canSendWriteWithoutResponse])
                                .filter{ $0 }
                                .prefix(1)
                                .map {_ in
                                    self.cbPeripheral.writeValue(data, for: charateristicUUIDFound!, type: .withoutResponse)
                                }
                                .setFailureType(to: LittleBluetoothError.self)
                                .map { charateristicUUIDFound! }
                                .eraseToAnyPublisher()
                            
                            return writeWOResp
                        }
                    }
                    .sink(receiveCompletion: { [unowned self, futKey] (completion) in
                        switch completion {
                        case .finished:
                            break
                        case .failure(let error):
                            promise(.failure(error))
                            self.removeAndCancelSubscriber(for: futKey)
                        }
                    }) { [unowned self, futKey] (readvalue) in
                        promise(.success(readvalue))
                        self.removeAndCancelSubscriber(for: futKey)
                    }
                    .store(in: disposeBag, for: futKey)
            }
        }
        .eraseToAnyPublisher()
        return write
    }

    
    func startListen(from charateristicUUID: CBUUID, of serviceUUID: CBUUID) -> AnyPublisher<CBCharacteristic, LittleBluetoothError> {
        
        let futKey = UUID()
        let notifyStart = Deferred {
            Future<CBCharacteristic, LittleBluetoothError> { [unowned self, futKey] promise in
                discoverCharacteristics([charateristicUUID], fromService: serviceUUID)
                    .flatMap { characteristics -> AnyPublisher<CBCharacteristic, LittleBluetoothError> in
                        let charateristicUUIDFound = characteristics.first(where: { charact in
                            charact.uuid == charateristicUUID
                        })
                        // Always re-enable notifications to ensure subscription after reconnection
                        defer {
                            self.cbPeripheral.setNotifyValue(true, for: charateristicUUIDFound!)
                        }
                        return self.peripheralProxy.peripheralUpdatedNotificationStateForCharacteristicPublisher
                            .tryMap { (value) -> CBCharacteristic in
                                switch value {
                                case let (_, error?):
                                    throw error
                                case let (charact, _):
                                    return charact
                                }
                            }
                            .mapError {$0 as! LittleBluetoothError}
                            .eraseToAnyPublisher()
                    }
                    .sink { (completion) in
                        switch completion {
                        case .finished:
                            break
                        case .failure(let error):
                            promise(.failure(error))
                            self.removeAndCancelSubscriber(for: futKey)
                        }
                    } receiveValue: { (charact) in
                        promise(.success(charact))
                        self.removeAndCancelSubscriber(for: futKey)
                    }
                    .store(in: self.disposeBag, for: futKey)
            }
            
        }
        .eraseToAnyPublisher()
        return notifyStart
    }
    
    func stopListen(from charateristicUUID: CBUUID, of serviceUUID: CBUUID) -> AnyPublisher<CBCharacteristic, LittleBluetoothError> {
        let futKey = UUID()
        let notifyStop = Deferred {
            Future<CBCharacteristic, LittleBluetoothError> { [unowned self, futKey] promise in
                discoverCharacteristics([charateristicUUID], fromService: serviceUUID)
                    .flatMap { characteristics -> AnyPublisher<CBCharacteristic, LittleBluetoothError> in
                        let charateristicUUIDFound = characteristics.first(where: { charact in
                            charact.uuid == charateristicUUID
                        })
                        if !charateristicUUIDFound!.isNotifying {
                            return Result<CBCharacteristic, LittleBluetoothError>.Publisher(.success(charateristicUUIDFound!))
                                .eraseToAnyPublisher()
                        }
                        defer {
                            self.cbPeripheral.setNotifyValue(false, for: charateristicUUIDFound!)
                        }
                        return self.peripheralProxy.peripheralUpdatedNotificationStateForCharacteristicPublisher
                            .tryMap { (value) -> CBCharacteristic in
                                switch value {
                                case let (_, error?):
                                    throw error
                                case let (charact, _):
                                    return charact
                                }
                            }
                            .mapError {$0 as! LittleBluetoothError}
                            .eraseToAnyPublisher()
                    }
                    .sink { (completion) in
                        switch completion {
                        case .finished:
                            break
                        case .failure(let error):
                            promise(.failure(error))
                            self.removeAndCancelSubscriber(for: futKey)
                        }
                    } receiveValue: { (charact) in
                        promise(.success(charact))
                        self.removeAndCancelSubscriber(for: futKey)
                    }
                    .store(in: self.disposeBag, for: futKey)
            }
        }
        .eraseToAnyPublisher()
        return notifyStop
    }
    
    func writeAndListen(from charateristicUUID: CBUUID, of serviceUUID: CBUUID, data: Data) -> AnyPublisher<Data?, LittleBluetoothError> {
        
        let futKey = UUID()
        let writeListen = Deferred {
            Future<Data?, LittleBluetoothError> { [unowned self, futKey] promise in
                startListen(from: charateristicUUID, of: serviceUUID)
                    .flatMap { (_) -> AnyPublisher<CBCharacteristic, LittleBluetoothError> in
                        self.write(to: charateristicUUID, of: serviceUUID, data: data)
                    }
                    .flatMap { (_) -> AnyPublisher<CBCharacteristic, LittleBluetoothError> in
                        self.peripheralProxy.peripheralUpdatedValueForNotifyCharacteristicPublisher
                            .tryMap { (value) -> CBCharacteristic in
                                switch value {
                                case let (_, error?):
                                    throw error
                                case let (charact, _):
                                    return charact
                                }
                            }
                            .mapError {$0 as! LittleBluetoothError}
                            .eraseToAnyPublisher()
                    }
                    .prefix(1)
                    .filter { (charachteristic) -> Bool in
                        if charachteristic.uuid == charateristicUUID {
                            return true
                        }
                        return false
                    }
                    .flatMap{ (_) -> AnyPublisher<CBCharacteristic, LittleBluetoothError> in
                        self.stopListen(from: charateristicUUID, of: serviceUUID)
                    }
                    .map { charact -> Data? in
                        charact.value
                    }
                    .sink { (completion) in
                        switch completion {
                        case .finished:
                            break
                        case .failure(let error):
                            promise(.failure(error))
                            self.removeAndCancelSubscriber(for: futKey)
                        }
                    } receiveValue: { (charact) in
                        promise(.success(charact))
                        self.removeAndCancelSubscriber(for: futKey)
                    }
                    .store(in: self.disposeBag, for: futKey)
            }
        }
        .eraseToAnyPublisher()
        
        return writeListen
    }
    
    func openL2CAPChannel(psm: CBL2CAPPSM) -> AnyPublisher<CBL2CAPChannel, LittleBluetoothError> {
        let futKey = UUID()
        let openChannel = Deferred {
            Future<CBL2CAPChannel, LittleBluetoothError> { [unowned self, futKey] promise in
                self.peripheralProxy.peripheralOpenedL2CAPChannelPublisher
                    .tryMap { (value) -> CBL2CAPChannel in
                        switch value {
                        case let (_, error?):
                            throw error
                        case let (channel?, _):
                            return channel
                        case (nil, nil):
                            throw LittleBluetoothError.couldNotOpenL2CAPChannel(error: NSError(domain: "LittleBlueTooth", code: -1, userInfo: [NSLocalizedDescriptionKey: "Unknown L2CAP channel opening error"]))
                        }
                    }
                    .mapError { $0 as! LittleBluetoothError }
                    .sink(receiveCompletion: { [unowned self, futKey] (completion) in
                        switch completion {
                        case .finished:
                            break
                        case .failure(let error):
                            promise(.failure(error))
                            self.removeAndCancelSubscriber(for: futKey)
                        }
                    }) { [unowned self, futKey] (channel) in
                        promise(.success(channel))
                        self.removeAndCancelSubscriber(for: futKey)
                    }
                    .store(in: self.disposeBag, for: futKey)
            }
        }
        .eraseToAnyPublisher()
        
        defer {
            #if TEST
            let result = LittleBlueToothTestHooks.l2capChannelProvider?(psm)
            let proxy = UncheckedSendableBox(peripheralProxy)
            DispatchQueue.main.async {
                switch result {
                case .success(let channel):
                    proxy.value.peripheralOpenedL2CAPChannelPublisher.send((channel, nil))
                case .failure(let error):
                    proxy.value.peripheralOpenedL2CAPChannelPublisher.send((nil, .couldNotOpenL2CAPChannel(error: error)))
                case nil:
                    let error = NSError(domain: "LittleBlueToothTest", code: -2,
                                        userInfo: [NSLocalizedDescriptionKey: "No l2capChannelProvider installed"])
                    proxy.value.peripheralOpenedL2CAPChannelPublisher.send((nil, .couldNotOpenL2CAPChannel(error: error)))
                }
            }
            #else
            cbPeripheral.openL2CAPChannel(psm)
            #endif
        }
        return openChannel
    }

    // MARK: - Public
    
    /// The maximum amount of data, in bytes, you can send to a characteristic in a single write type.
    public func maxWriteValueLengthWithResponse(_ response: Bool ) -> Int {
        cbPeripheral.maximumWriteValueLength(for: response ? .withResponse : .withoutResponse)
    }
    
}

extension Peripheral: CustomDebugStringConvertible {
    /// Extended description of the peripheral
    public var debugDescription: String {
        return """
        Id: \(id)
        Name: \(name ?? "Not available")
        CBPeripheral: \(cbPeripheral)
        """
    }
}

