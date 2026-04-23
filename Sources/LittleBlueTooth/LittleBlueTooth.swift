//
//  LittleBlueTooth.swift
//  LittleBlueTooth
//
//  Created by Andrea Finollo on 10/06/2020.
//  Copyright © 2020 Andrea Finollo. All rights reserved.
//

import Foundation
import Combine
#if TEST
@preconcurrency import CoreBluetoothMock
#else
@preconcurrency import CoreBluetooth
#endif

public protocol Readable {
    init(from data: Data) throws
}

public protocol Writable {
    var data: Data {get}
}

/**
`LittleBlueTooth` can control only one peripheral at time. It has an `id` properties to identifiy different instances.
Please note that Apple do not enacourage the use of more `CBCentralManger` instances, due to resurce hits.
 [Link](https://developer.apple.com/forums/thread/20810)
 */
public final class LittleBlueTooth: Identifiable, @unchecked Sendable {
    
    // MARK: - Public variables
    /// LittleBlueTooth instance identifier
    public let id = UUID()
    
    /// This is usefull when you have auto-reconnection and want to do some task right after a connection.
    /// All other tasks will be delayed until this one ends.
    public var connectionTasks: AnyPublisher<Void, LittleBluetoothError>?
    
    /// This handler must be used to handle connection process after a disconnession.
    /// You can inspect the error and decide if an automatic connection is necessary.
    /// If you return `true` the connection process will start, once the peripheral has been found a connection will be established.
    /// If you return `false` iOS will not try to establish a connection
    /// Connection process will remain active also in background if the app has the right
    /// permission, to cancel just call `disconnect`.
    /// When a connection will be established an `.autoConnected(PeripheralIdentifier)` event will be streamed to
    /// the `connectionEventPublisher`
    public var autoconnectionHandler: AutoconnectionHandler?
    
    /// Connected peripheral. `nil` if not connected or a connection is not requested
    public var peripheral: Peripheral? {
        didSet {
            guard let per = peripheral else {
                logHandler?("peripheral set to nil", .trace, .connection)
                return
            }
            per.isLogEnabled = isLogEnabled
            per.logHandler = logHandler
            logHandler?("peripheral set: \(per.cbPeripheral.identifier)", .trace, .connection)
        }
    }
    
    /// Publisher that streams peripheral state  available only when a connection is requested for fine grained control.
    /// This subject survives disconnect/reconnect cycles so external subscribers
    /// don't lose events when the internal connectable publisher is recreated.
    private let peripheralStateSubject = PassthroughSubject<PeripheralState, Never>()
    public lazy var peripheralStatePublisher: AnyPublisher<PeripheralState, Never> = {
        peripheralStateSubject.eraseToAnyPublisher()
    }()

    /// Publisher that streams `ConnectionEvent`
    public lazy var connectionEventPublisher: AnyPublisher<ConnectionEvent, Never> = { [unowned self] in
        return self.centralProxy.connectionEventPublisher.share().eraseToAnyPublisher()
    }()
    
    /// Publisher that streams CBCentralManager state changes
    public var centralStatePublisher: AnyPublisher<BluetoothState, Never> {
        centralProxy.centralStatePublisher.eraseToAnyPublisher()
    }
    
    /// Publish name and service changes
    public var changesStatePublisher: AnyPublisher<PeripheralChanges, Never> {
        _peripheralChangesPublisher.eraseToAnyPublisher()
    }
    
    /// Publish all values from `LittleBlueToothCharacteristic` that you are already listening to.
    /// It's up to you to filter them and convert raw data to the `Readable` object.
    public var listenPublisher: AnyPublisher<LittleBlueToothCharacteristic, LittleBluetoothError> {
        return _listenPublisher
            .map { (characteristic) -> LittleBlueToothCharacteristic in
                LittleBlueToothCharacteristic(with: characteristic)
            }
            .eraseToAnyPublisher()
    }
    
    public var restoreStatePublisher: AnyPublisher<CentralRestorer, Never> {
        return centralProxy.willRestoreStatePublisher
    }
    
    /// Enable logging disabled by default
    /// Enable logging, log is made using os_log and it exposes some information even in release configuration
    public var isLogEnabled: Bool {
        get {
            return _isLogEnabled
        }
        set {
            _isLogEnabled = newValue
        }
    }
    
    // MARK: - Private variables
    /// The original CBPeripheral from state restoration. Survives disconnect cleanup
    /// so reconnection can reuse it instead of retrieving a new proxy.
    private var restoredCBPeripheral: CBPeripheral?
    /// Cancellable operation identified by a `UUID` key. Thread-safe:
    /// the central manager queue and caller queues both mutate it.
    private let disposeBag = SubscriptionBag()
    /// Scan cancellable operation
    private var scanning: AnyCancellable?
    /// Peripheral state  publisher. It will be created after `Peripheral` instance creation.
    private var peripheralStatePublisherCancellable: Cancellable?
    /// Forwards internal peripheral state events to the stable peripheralStateSubject
    private var peripheralStatePipeCancellable: AnyCancellable?
    /// Peripheral changes  publisher. It will be created after `Peripheral` instance creation.
    private var peripheralChangesPublisherCancellable: Cancellable?
    /// Notification  publisher. It will be created after `Peripheral` instance creation.
    private var listenPublisherCancellable: Cancellable?
    /// Cancellable connection event subscriber.
    private var connectionEventSubscriber: AnyCancellable?

    private var _listenPublisher: Publishers.Multicast<AnyPublisher<CBCharacteristic, LittleBluetoothError>, PassthroughSubject<CBCharacteristic, LittleBluetoothError>> {
        if _listenPublisher_ == nil {
            let pub =
                ensureBluetoothState()
                .flatMapLatest { [unowned self] _ in
                    self.ensurePeripheralReady()
                }
                .flatMapLatest { [unowned self] _ -> AnyPublisher<CBCharacteristic, LittleBluetoothError> in
                    guard let peri = self.peripheral else {
                        return Fail(error: .peripheralNotConnectedOrAlreadyDisconnected).eraseToAnyPublisher()
                    }
                    return peri.listenPublisher
                }
                .share()
                .eraseToAnyPublisher()
            
            _listenPublisher_ = Publishers.Multicast(upstream: pub, createSubject:{ PassthroughSubject() })
            return _listenPublisher_!
        }
        return _listenPublisher_!
    }
    
    private var _listenPublisher_: Publishers.Multicast<AnyPublisher<CBCharacteristic, LittleBluetoothError>, PassthroughSubject<CBCharacteristic, LittleBluetoothError>>?
    
    /// Peripheral state connectable publisher. It will be connected after `Peripheral` instance creation.
    private var _peripheralStatePublisher: Publishers.MakeConnectable<AnyPublisher<PeripheralState, Never>> {
        if _peripheralStatePublisher_ == nil {
            _peripheralStatePublisher_ =  Just(())
            .flatMapLatest {
                self.peripheral!.peripheralStatePublisher
            }
            .eraseToAnyPublisher()
            .makeConnectable()
            return _peripheralStatePublisher_!
        }
        return _peripheralStatePublisher_!
    }
    
    private var _peripheralStatePublisher_: Publishers.MakeConnectable<AnyPublisher<PeripheralState, Never>>?


    /// Peripheral changes connectable publisher. It will be connected after `Peripheral` instance creation.
    private var _peripheralChangesPublisher: Publishers.MakeConnectable<AnyPublisher<PeripheralChanges, Never>> {
        if _peripheralChangesPublisher_ == nil {
            _peripheralChangesPublisher_ =
                Just(())
                .flatMapLatest {
                    self.peripheral!.changesPublisher
                }
                .eraseToAnyPublisher()
                .makeConnectable()
            return _peripheralChangesPublisher_!
            
        }
        return _peripheralChangesPublisher_!
    }
    
    private var _peripheralChangesPublisher_: Publishers.MakeConnectable<AnyPublisher<PeripheralChanges, Never>>?
    
    private var restoreStateCancellable: AnyCancellable?
    private var _isLogEnabled: Bool = false
    private var autoconnectionOptions: [String : Any]?
    var logHandler: LBTLogHandler?

    var cbCentral: CBCentralManager
    var centralProxy = CBCentralManagerDelegateProxy()
    
    // MARK: - Init
    public init(with configuration: LittleBluetoothConfiguration) {
        #if TEST
        self.cbCentral = CBCentralManagerFactory.instance(delegate: self.centralProxy, queue: configuration.centralManagerQueue, options: configuration.centralManagerOptions, forceMock: true)
        #else
        self.cbCentral = CBCentralManager(delegate: self.centralProxy, queue: configuration.centralManagerQueue, options: configuration.centralManagerOptions)
        #endif
        self.autoconnectionHandler = configuration.autoconnectionHandler
        self.autoconnectionOptions = configuration.autoconnectionOptions
        self.logHandler = configuration.logHandler
        self.centralProxy.logHandler = configuration.logHandler
        if (configuration.restoreHandler == nil &&
            configuration.centralManagerOptions?[CBCentralManagerOptionRestoreIdentifierKey] != nil) ||
            (configuration.restoreHandler != nil &&
                configuration.centralManagerOptions?[CBCentralManagerOptionRestoreIdentifierKey] == nil) {
            print("If you want to use state preservation/restoration you should probably want to implement the `restoreHandler`")
        }
        attachSubscribers(with: configuration.restoreHandler)
        self.isLogEnabled = configuration.isLogEnabled
        emit("Initialized with options: \(configuration.centralManagerOptions?.description ?? "none")", .debug, .connection)
    }
    
    func attachSubscribers(with restorehandler: ((Restored) -> Void)?) {
        self.connectionEventSubscriber =
            connectionEventPublisher
            .flatMap { [unowned self] (event) -> AnyPublisher<ConnectionEvent, Never> in
                self.logHandler?("Connection event: \(event)", .trace, .connection)
                switch event {
                case .connected(let periph),
                     .autoConnected(let periph):
                    self.emit("attachSubscribers: handling \(event)", .trace, .connection)
                    self.listenPublisherCancellable = self._listenPublisher.connect()

                    if let connTask = self.connectionTasks {
                        // I'm doing a copy of the connectionTask so if something fails
                        // next time it will start over.
                        // TEMPORARY WORKAROUND: Those Dispatch async will make the states flow correctly in the process: first connect, then ready. Without it would be the contrary
                        return AnyPublisher(connTask)
                            .catch { [unowned self] (error) -> Just<Void> in
                                DispatchQueue.main.async {
                                    self.centralProxy.connectionEventPublisher.send(ConnectionEvent.notReady(periph, error: error))
                                }
                                return Just(())
                        }
                        .map { _ in
                            DispatchQueue.main.async {
                                self.centralProxy.connectionEventPublisher.send(ConnectionEvent.ready(periph))
                            }
                            return event
                        }
                        .eraseToAnyPublisher()
                    } else {
                        DispatchQueue.main.async {
                            self.centralProxy.connectionEventPublisher.send(ConnectionEvent.ready(periph))
                        }
                        return Just(event).eraseToAnyPublisher()
                    }
                default:
                    return Just(event).eraseToAnyPublisher()
                }
            }
                // This delay to make able other subscribers to receive notification
            .delay(for: .milliseconds(50), scheduler: DispatchQueue.main)
            .sink { [unowned self] (event) in
//                print("Sinking event \(event)")
                // Handle disconnection — trigger autoconnection if configured
                if case ConnectionEvent.disconnected( let peripheral, let error) = event {
                    self.logHandler?("Disconnected: \(peripheral.identifier), error: \(error?.localizedDescription ?? "none")", .info, .connection)
                    self.peripheralStateSubject.send(.disconnected)
                    self.cleanUpForDisconnection()
                    if let autoCon = self.autoconnectionHandler {
                        let periph = PeripheralIdentifier(peripheral: peripheral)
                        let shouldReconnect = autoCon(periph, error)
                        self.logHandler?("Autoconnection handler returned \(shouldReconnect) for \(periph.id.uuidString)", .debug, .connection)
                        if shouldReconnect {
                            self.logHandler?("Initiating reconnect for \(periph.id.uuidString)", .info, .connection)
                            self.autoconnect(to: periph)
                        }
                    }
                }
                // Handle connection failure (e.g., encryption error) — re-issue connect
                // cbCentral.connect() is fire-and-forget, but didFailToConnect means CB
                // gave up. We need to try again.
                if case ConnectionEvent.connectionFailed(let peripheral, let error) = event {
                    self.logHandler?("Connection failed: \(peripheral.identifier), error: \(error?.localizedDescription ?? "none")", .warning, .connection)
                    if let autoCon = self.autoconnectionHandler {
                        let periph = PeripheralIdentifier(peripheral: peripheral)
                        if autoCon(periph, error) {
                            self.logHandler?("Re-issuing autoconnect after connection failure", .info, .connection)
                            self.autoconnect(to: periph)
                        }
                    }
                }
        }
        if let handler = restorehandler {
            self.restoreStateCancellable = centralProxy.willRestoreStatePublisher
            .map { [unowned self] (restorer) -> Restored in
                let restored = self.restore(restorer)
                return restored
            }
            .sink(receiveValue: { (restored) in
                handler(restored)
            })
        }
    }
    
    deinit {
        print("Deinit: \(self)")
        disposeBag.cancelAll()
        scanning?.cancel()
        connectionEventSubscriber?.cancel()
        guard let peri = peripheral else {
            return
        }
        cbCentral.cancelPeripheralConnection(peri.cbPeripheral)

    }
    // MARK: - Peripheral state bridge

    /// Connects the internal connectable peripheral state publisher and
    /// pipes its values into the stable `peripheralStateSubject` so that
    /// external subscribers survive disconnect/reconnect cycles.
    private func connectPeripheralStatePublisher() {
        peripheralStatePipeCancellable?.cancel()
        // Subscribe BEFORE calling connect() — MakeConnectable only
        // forwards to subscribers that exist when connect() is called.
        peripheralStatePipeCancellable = _peripheralStatePublisher
            .sink { [weak self] state in
                self?.peripheralStateSubject.send(state)
            }
        peripheralStatePublisherCancellable = _peripheralStatePublisher.connect()
    }

    // MARK: - Autoconnection (direct cbCentral.connect)

    /// Per Apple best practices: call cbCentral.connect() directly in the
    /// disconnect handler. The connection request stays pending forever —
    /// no timeout, no retry loop, no publisher chain. When the peripheral
    /// becomes available, didConnect fires and the existing
    /// connectionEventPublisher flow handles the rest.
    private func autoconnect(to periph: PeripheralIdentifier) {
        // Retrieve the CBPeripheral from CoreBluetooth
        let cbPeripheral: CBPeripheral
        if let restored = self.restoredCBPeripheral, restored.identifier == periph.id {
            cbPeripheral = restored
        } else {
            let retrieved = self.cbCentral.retrievePeripherals(withIdentifiers: [periph.id])
            guard let found = retrieved.first else {
                logHandler?("Autoconnection failed: peripheral not found for \(periph.id.uuidString)", .error, .connection)
                return
            }
            cbPeripheral = found
        }

        // Set up the Peripheral wrapper and publishers so connectionEventPublisher
        // subscribers see the state transitions when didConnect fires.
        self.peripheral = Peripheral(cbPeripheral)
        self.peripheral!.skipServiceCache = true
        self.peripheral!.isLogEnabled = self.isLogEnabled
        // Re-assert delegate after all old Peripheral references are released.
        // ARC may defer deallocation of a previous Peripheral whose proxy held this
        // CBPeripheral's delegate as a weak reference. Setting it again here ensures
        // the new proxy wins.
        self.peripheral!.reassertDelegate()
        emit("autoconnect: Peripheral ready, delegate=\(cbPeripheral.delegate != nil)", .trace, .connection)
        self.connectPeripheralStatePublisher()
        self.peripheralChangesPublisherCancellable = self._peripheralChangesPublisher.connect()
        self.centralProxy.isAutoconnectionActive = true

        // Fire-and-forget. CB keeps this request alive indefinitely.
        // didConnect → connectionEventPublisher → attachSubscribers handles the rest.
        emit("autoconnect: calling cbCentral.connect with options: \(autoconnectionOptions ?? [:])", .debug, .connection)
        self.cbCentral.connect(cbPeripheral, options: autoconnectionOptions)
    }

    // MARK: - RSSI
    public func readRSSI() -> AnyPublisher<Int, LittleBluetoothError> {
        let rssiSubject = PassthroughSubject<Int, LittleBluetoothError>()
        let key = UUID()
        
        self.ensureBluetoothState()
        .log(self, "Read RSSI", .debug, .gatt)
        .flatMap { [unowned self] _ in
            self.ensurePeripheralReady()
        }
        .flatMap { [unowned self] _ -> AnyPublisher<Int, LittleBluetoothError> in
            guard let peri = self.peripheral else {
                return Fail(error: .peripheralNotConnectedOrAlreadyDisconnected).eraseToAnyPublisher()
            }
            return peri.readRSSI()
        }
        .sink(receiveCompletion: { [unowned self, key] (completion) in
            switch completion {
            case .finished:
                break
            case .failure(let error):
                rssiSubject.send(completion: .failure(error))
                self.removeAndCancelSubscriber(for: key)
            }
        }) { [unowned self, key] (rssi) in
            rssiSubject.send(rssi)
            rssiSubject.send(completion: .finished)
            self.removeAndCancelSubscriber(for: key)
        }
        .store(in: disposeBag, for: key)
        
        return rssiSubject.eraseToAnyPublisher()
    }

    // MARK: - Listen
    
    /// Returns a multicast publisher once you attached all the subscriber you must call `connect()`
    /// - parameter characteristic: Characteristc you want to be notified.
    /// - parameter valueType: The type of the value you want the raw `Data` be converted
    /// - returns: A multicast publisher that will send out values of the type you choose.
    /// - important: The type of the value must be conform to `Readable`
    public func connectableListenPublisher<T: Readable>(for characteristic: LittleBlueToothCharacteristic, valueType: T.Type) -> Publishers.MakeConnectable<AnyPublisher<T, LittleBluetoothError>> {
        
           let listen = ensureBluetoothState()
           .log(self, "ConnectableListen \(characteristic.id)", .debug, .gatt)
           .flatMap { [unowned self] _ in
               self.ensurePeripheralReady()
           }
           .flatMap { (periph) -> AnyPublisher<(CBCharacteristic, Peripheral), LittleBluetoothError> in
               return periph.startListen(from: characteristic.id, of: characteristic.service)
                   .map { (characteristic) -> (CBCharacteristic, Peripheral) in
                       (characteristic, periph)
               }
               .eraseToAnyPublisher()
           }
           .flatMap { (_ ,periph) in
               periph.listenPublisher
           }
           .filter { charact -> Bool in
             charact.uuid == characteristic.id
           }
           .tryMap { (characteristic) -> T in
               guard let data = characteristic.value else {
                   throw LittleBluetoothError.emptyData
               }
               return try T.init(from: data)
           }.mapError { (error) -> LittleBluetoothError in
               if let er = error as? LittleBluetoothError {
                   return er
               }
               return .emptyData
           }
           .share()
           .eraseToAnyPublisher()
           
        return  Publishers.MakeConnectable(upstream: listen)
       }
    
       
    /// Returns a shared publisher for listening to a specific characteristic.
    /// - parameter characteristic: Characteristc you want to be notified.
    /// - returns: A shared publisher that will send out values of the type defined by the generic type.
    /// - important: The type of the value must be conform to `Readable`
    public func startListen<T: Readable>(from charact: LittleBlueToothCharacteristic) -> AnyPublisher<T, LittleBluetoothError> {
        let lis = ensureBluetoothState()
        .log(self, "StartListen \(charact.id)", .debug, .gatt)
        .flatMap { [unowned self] _ in
            self.ensurePeripheralReady()
        }
        .flatMap { (periph) -> AnyPublisher<(LittleBlueToothCharacteristic, Peripheral), LittleBluetoothError> in
            return periph.startListen(from: charact.id, of: charact.service)
                .map { (characteristic) -> (LittleBlueToothCharacteristic, Peripheral) in
                    (LittleBlueToothCharacteristic(with: characteristic), periph)
            }
            .eraseToAnyPublisher()
        }
        .flatMap { (_ ,periph) in
            periph.listenPublisher
        }
        .filter { characteristic -> Bool in
            charact.id == characteristic.uuid
        }
        .tryMap { (characteristic) -> T in
            guard let data = characteristic.value else {
                throw LittleBluetoothError.emptyData
            }
            return try T.init(from: data)
        }.mapError { (error) -> LittleBluetoothError in
            if let er = error as? LittleBluetoothError {
                return er
            }
            return .emptyData
        }
        .eraseToAnyPublisher()
        return lis
        
    }
    
    /// Returns a  publisher with the `LittleBlueToothCharacteristic` where the notify command has been activated.
    /// After starting the listen command you should subscribe to the `listenPublisher` to be notified.
    /// - parameter characteristic: Characteristc you want to be notified.
    /// - returns: A  publisher with the `LittleBlueToothCharacteristic` where the notify command has been activated.
    /// - important: This publisher only activate the notification on a specific characteristic, it will not send notified values.
    /// After starting the listen command you should subscribe to the `listenPublisher` to be notified.
    public func enableListen(from characteristic: LittleBlueToothCharacteristic) -> AnyPublisher<LittleBlueToothCharacteristic, LittleBluetoothError> {
        
        let startListenSubject = PassthroughSubject<LittleBlueToothCharacteristic, LittleBluetoothError>()
        let key = UUID()
        
        self.ensureBluetoothState()
        .log(self, "EnableListen \(characteristic.id)", .debug, .gatt)
        .flatMap { [unowned self] _ in
            self.ensurePeripheralReady()
        }
        .flatMap { (periph) -> AnyPublisher<CBCharacteristic, LittleBluetoothError> in
            periph.startListen(from: characteristic.id, of: characteristic.service)
        }
        .sink(receiveCompletion: { [unowned self, key] (completion) in
            switch completion {
            case .finished:
                break
            case .failure(let error):
                startListenSubject.send(completion: .failure(error))
                self.removeAndCancelSubscriber(for: key)
            }
        }) { [unowned self, key] (characteristic) in
            startListenSubject.send(LittleBlueToothCharacteristic(with: characteristic))
            startListenSubject.send(completion: .finished)
            self.removeAndCancelSubscriber(for: key)
        }
        .store(in: disposeBag, for: key)
        
        return startListenSubject.eraseToAnyPublisher()
    }
    
    
    /// Disable listen from a specific characteristic
    /// - parameter characteristic: characteristic you want to stop listen
    /// - returns: A publisher with that informs you about the successful or failed task
    public func disableListen(from characteristic: LittleBlueToothCharacteristic) -> AnyPublisher<LittleBlueToothCharacteristic, LittleBluetoothError> {
        
        let stopSubject = PassthroughSubject<LittleBlueToothCharacteristic, LittleBluetoothError>()
        
        let key = UUID()
        ensureBluetoothState()
        .flatMap { [unowned self] _ in
            self.ensurePeripheralReady()
        }
        .flatMap { (periph) -> AnyPublisher<CBCharacteristic, LittleBluetoothError> in
            periph.stopListen(from: characteristic.id, of: characteristic.service)
        }
        .sink(receiveCompletion: { [unowned self, key] (completion) in
            switch completion {
            case .finished:
                break
            case .failure(let error):
                stopSubject.send(completion: .failure(error))
                self.removeAndCancelSubscriber(for: key)
            }
        }) { [unowned self, key] (readvalue) in
            stopSubject.send(LittleBlueToothCharacteristic(with:readvalue))
            stopSubject.send(completion: .finished)
            self.removeAndCancelSubscriber(for: key)
        }
        .store(in: disposeBag, for: key)
        
        return stopSubject.eraseToAnyPublisher()
    }

    // MARK: - Read
    
    /// Read a value from a specific charteristic
    /// - parameter characteristic: characteristic where you want to read
    /// - returns: A publisher with the value you want to read.
    /// - important: The type of the value must be conform to `Readable`
    public func read<T: Readable>(from characteristic: LittleBlueToothCharacteristic) -> AnyPublisher<T, LittleBluetoothError> {
        
        let readSubject = PassthroughSubject<T, LittleBluetoothError>()
        let futKey = UUID()
        ensureBluetoothState()
            .log(self, "Read from \(characteristic.id)", .debug, .gatt)
            .flatMap { [unowned self] _ in
                self.ensurePeripheralReady()
            }
            .flatMap { [characteristic] periph in
                periph.read(from: characteristic.id, of: characteristic.service)
            }
            .tryMap { (data) -> T in
                guard let data = data else {
                    throw LittleBluetoothError.emptyData
                }
                return try T.init(from: data)
            }
            .mapError { (error) -> LittleBluetoothError in
                if let er = error as? LittleBluetoothError {
                    return er
                }
                return .couldNotReadFromCharacteristic(characteristic: characteristic.id, error: error)
            }
            .sink(receiveCompletion: { [unowned self, futKey] (completion) in
                switch completion {
                case .finished:
                    break
                case .failure(let error):
                    readSubject.send(completion: .failure(error))
                    self.removeAndCancelSubscriber(for: futKey)
                }
            }) { [unowned self, futKey] (readvalue) in
                readSubject.send(readvalue)
                readSubject.send(completion: .finished)
                self.removeAndCancelSubscriber(for: futKey)
            }
            .store(in: disposeBag, for: futKey)
        
        return readSubject.eraseToAnyPublisher()
    }
    
    
   // MARK: - Write

    /// Write a value to a specific charteristic
    /// - parameter characteristic: characteristic where you want to write
    /// - parameter value: The value you want to write
    /// - parameter response: An optional `Bool` value that will look for error after write operation
    /// - returns: A publisher with that informs you about eventual error
    /// - important: The type of the value must be conform to `Writable`
    public func write<T: Writable>(to characteristic: LittleBlueToothCharacteristic, value: T, response: Bool = true) -> AnyPublisher<Void, LittleBluetoothError> {
        
        let writeSubject = PassthroughSubject<Void, LittleBluetoothError>()
        let key = UUID()

        ensureBluetoothState()
        .log(self, "Write to \(characteristic.id)", .debug, .gatt)
        .flatMap { [unowned self] _ in
            self.ensurePeripheralReady()
        }
        .flatMap { periph in
            periph.write(to: characteristic.id, of: characteristic.service, data: value.data, response: response)
        }
        .sink(receiveCompletion: { [unowned self, key] (completion) in
            switch completion {
            case .finished:
                break
            case .failure(let error):
                writeSubject.send(completion: .failure(error))
                self.removeAndCancelSubscriber(for: key)
            }
        }, receiveValue: { [unowned self, key] (_) in
            writeSubject.send(())
            writeSubject.send(completion: .finished)
            self.removeAndCancelSubscriber(for: key)
        })
        .store(in: disposeBag, for: key)
        
        return writeSubject.eraseToAnyPublisher()
    }
    
    
    /// Write a value to a specific charteristic and wait for a response
    /// - parameter characteristic: characteristic where you want to write and listen
    /// - parameter value: The value you want to write must conform to `Writable`
    /// - returns: A publisher with that post and error or the response of the write requests.
    /// - important: Written value must conform to `Writable`, response must conform to `Readable`
    public func writeAndListen<W: Writable, R: Readable>(from characteristic: LittleBlueToothCharacteristic, value: W) -> AnyPublisher<R, LittleBluetoothError> {

        let writeListenSubject = PassthroughSubject<R, LittleBluetoothError>()
        let key = UUID()

        ensureBluetoothState()
        .log(self, "WriteAndListen \(characteristic.id)", .debug, .gatt)
        .flatMap { [unowned self] _ in
            self.ensurePeripheralReady()
        }
        .flatMap { (periph) in
            periph.writeAndListen(from: characteristic.id, of: characteristic.service, data: value.data)
        }
        .tryMap { (data) -> R in
            guard let data = data else {
                throw LittleBluetoothError.emptyData
            }
            return try R(from: data)
        }.mapError { (error) -> LittleBluetoothError in
            if let er = error as? LittleBluetoothError {
                return er
            }
            return .couldNotReadFromCharacteristic(characteristic: characteristic.id, error: error)
        }
        .sink(receiveCompletion: { [unowned self, key] (completion) in
            switch completion {
            case .finished:
                break
            case .failure(let error):
                writeListenSubject.send(completion: .failure(error))
                self.removeAndCancelSubscriber(for: key)
            }
        }) { [unowned self, key] (readvalue) in
            writeListenSubject.send(readvalue)
            writeListenSubject.send(completion: .finished)
            self.removeAndCancelSubscriber(for: key)
        }
        .store(in: disposeBag, for: key)
        
        return writeListenSubject.eraseToAnyPublisher()
    }
    
    // MARK: - Discover devices

    /// Starts scanning for `PeripheralDiscovery`
    /// - parameter services: Services for peripheral you are looking for
    /// - parameter options: Scanning options same as  CoreBluetooth  central manager option.
    /// - returns: A publisher with stream of disovered peripherals.
    public func startDiscovery(withServices services: [CBUUID]?, options: [String : Any]? = nil) -> AnyPublisher<PeripheralDiscovery, LittleBluetoothError> {
        if self.cbCentral.isScanning {
            self.cbCentral.stopScan()
//            return Result<PeripheralDiscovery, LittleBluetoothError>.Publisher(.failure(.alreadyScanning)).eraseToAnyPublisher()
        }
        
        let scanSubject = PassthroughSubject<PeripheralDiscovery, LittleBluetoothError>()

        scanning =
        ensureBluetoothState()
        .log(self, "Scanning for peripherals", .debug, .scan)
        .map { [unowned self] _  -> Void in
            if self.cbCentral.isScanning {
                self.cbCentral.stopScan()
            }
            return ()
        }
        .flatMap { [unowned self] _  -> Publishers.SetFailureType<PassthroughSubject<PeripheralDiscovery, Never>, LittleBluetoothError> in
            self.cbCentral.scanForPeripherals(withServices: services, options: options)
            return self.centralProxy.centralDiscoveriesPublisher.setFailureType(to: LittleBluetoothError.self)
        }
        .sink(receiveCompletion: { [unowned self] (completion) in
            switch completion {
            case .finished:
                break
            case .failure(let error):
                scanSubject.send(completion: .failure(error))
                self.cbCentral.stopScan()
                self.scanning?.cancel()
                self.scanning = nil
            }
        }) { (discovery) in
            scanSubject.send(discovery)
        }
        
        return scanSubject.eraseToAnyPublisher()
    }
    
    /// Stops peripheral discovery
    /// - returns: A publisher when discovery has been stopped
    public func stopDiscovery() -> AnyPublisher<Void, LittleBluetoothError> {
        return Deferred {
            Future<Void, LittleBluetoothError> { [unowned self] promise in
                self.cbCentral.stopScan()
                self.scanning?.cancel()
                self.scanning = nil
                promise(.success(()))
            }
        }.eraseToAnyPublisher()
    }
    
    // MARK: - Connect
    
    /// Starts connection for `PeripheralIdentifier`
    /// - parameter options: Connecting options same as  CoreBluetooth  central manager option.
    /// - returns: A publisher with the just connected `Peripheral`.
    public func connect(to peripheralIdentifier: PeripheralIdentifier, options: [String : Any]? = nil) -> AnyPublisher<Peripheral, LittleBluetoothError> {
        return connect(to: peripheralIdentifier, options: options, autoreconnect: false)
    }
    
    /// Starts connection for `PeripheralDiscovery`
    /// - parameter options: Connecting options same as  CoreBluetooth  central manager option.
    /// - returns: A publisher with the just connected `Peripheral`.
    public func connect(to discovery: PeripheralDiscovery, options: [String : Any]? = nil) -> AnyPublisher<Peripheral, LittleBluetoothError> {
        if cbCentral.isScanning {
            scanning?.cancel()
            scanning = nil
            cbCentral.stopScan()
        }
        let peripheralIdentifier = PeripheralIdentifier(peripheral: discovery.cbPeripheral)
        
        return connect(to: peripheralIdentifier, options: options)
    }
    
    private func connect(to peripheralIdentifier: PeripheralIdentifier, options: [String : Any]? = nil, autoreconnect: Bool) -> AnyPublisher<Peripheral, LittleBluetoothError> {
        if let periph = peripheral, periph.state == .connecting || periph.state == .connected {
            return Result<Peripheral, LittleBluetoothError>.Publisher(.failure(.peripheralAlreadyConnectedOrConnecting(periph))).eraseToAnyPublisher()
        }
        
        let connectSubject = PassthroughSubject<Peripheral, LittleBluetoothError>()
        let key = UUID()
        
        ensureBluetoothState()
        .log(self, "Connecting to \(peripheralIdentifier.id)", .debug, .connection)
        .tryMap { [unowned self] _ -> Void in
            // Reuse the original CBPeripheral from state restoration if it matches,
            // or the existing wrapper. This preserves the delegate that CoreBluetooth
            // tracks internally, avoiding the "delegate is nil" API misuse error.
            let cbPeripheral: CBPeripheral
            if let existing = self.peripheral, existing.cbPeripheral.identifier == peripheralIdentifier.id {
                cbPeripheral = existing.cbPeripheral
            } else if let restored = self.restoredCBPeripheral, restored.identifier == peripheralIdentifier.id {
                cbPeripheral = restored
                self.peripheral = Peripheral(cbPeripheral)
                self.peripheral!.skipServiceCache = true
                self.peripheral!.isLogEnabled = self.isLogEnabled
            } else {
                let filtered = self.cbCentral.retrievePeripherals(withIdentifiers: [peripheralIdentifier.id]).filter { (periph) -> Bool in
                    periph.identifier == peripheralIdentifier.id
                }
                if filtered.isEmpty {
                    throw LittleBluetoothError.peripheralNotFound
                }
                cbPeripheral = filtered.first!
                self.peripheral = Peripheral(cbPeripheral)
                self.peripheral!.isLogEnabled = self.isLogEnabled
            }
            self.peripheral?.reassertDelegate()
            self.connectPeripheralStatePublisher()
            self.peripheralChangesPublisherCancellable = self._peripheralChangesPublisher.connect()
            self.centralProxy.isAutoconnectionActive = autoreconnect
            self.emit("connect(): calling cbCentral.connect", .debug, .connection)
            self.cbCentral.connect(cbPeripheral, options: options)
        }.mapError { error in
            error as! LittleBluetoothError
        }
        .flatMap { [unowned self] _ in
            self.centralProxy.connectionEventPublisher.setFailureType(to: LittleBluetoothError.self)
        }
        .filter{ (event) -> Bool in
//            print("Connct to event: \(event)")
            switch event {
                case .connected( _),
                     .autoConnected( _):
               return false
            default:
                return true
            }
        }
        .prefix(1)
        .tryMap { [unowned self] (event) -> Peripheral in
            switch event {
            case .ready:
                guard let peripheral = self.peripheral else {
                    throw LittleBluetoothError.peripheralNotConnectedOrAlreadyDisconnected
                }
                return peripheral
            case .notReady(_, let error?):
                if let tracked = self.peripheral { self.cbCentral.cancelPeripheralConnection(tracked.cbPeripheral) }
                self.peripheral = nil
                throw error
            case .connectionFailed(_, let error?):
                if let tracked = self.peripheral { self.cbCentral.cancelPeripheralConnection(tracked.cbPeripheral) }
                self.peripheral = nil
                throw error
            case .connectionFailed(let periph, _):
                if let tracked = self.peripheral { self.cbCentral.cancelPeripheralConnection(tracked.cbPeripheral) }
                self.peripheral = nil
                throw LittleBluetoothError.couldNotConnectToPeripheral(PeripheralIdentifier(peripheral: periph), nil)
            case .disconnected(_, let error?):
                if let tracked = self.peripheral { self.cbCentral.cancelPeripheralConnection(tracked.cbPeripheral) }
                self.peripheral = nil
                throw error
            case .disconnected(let periph, _):
                if let tracked = self.peripheral { self.cbCentral.cancelPeripheralConnection(tracked.cbPeripheral) }
                self.peripheral = nil
                throw LittleBluetoothError.peripheralDisconnected(PeripheralIdentifier(peripheral: periph), nil)
            default:
                fatalError("Connection event not handled")
            }
        }
        .mapError { (error) -> LittleBluetoothError in
                error as! LittleBluetoothError
        }
        .sink(receiveCompletion: { [unowned self, key] (completion) in
            switch completion {
            case .finished:
                break
            case .failure(let error):
                connectSubject.send(completion: .failure(error))
                self.peripheral = nil
                self.removeAndCancelSubscriber(for: key)
            }
        }, receiveValue: { [unowned self, key] (peripheral) in
            connectSubject.send(peripheral)
            connectSubject.send(completion: .finished)
            self.removeAndCancelSubscriber(for: key)
        })
        .store(in: disposeBag, for: key)

        return connectSubject
            .handleEvents(receiveCancel: { [unowned self] in
                if let periph = self.peripheral {
                    self.emit("connect(): subscriber cancelled — cancelling CB pending connect", .debug, .connection)
                    self.cbCentral.cancelPeripheralConnection(periph.cbPeripheral)
                    self.peripheral = nil
                }
            })
            .eraseToAnyPublisher()
    }

    // MARK: - Disconnect

    /// Disconnect the connected `Peripheral`
    /// - returns: A publisher with the just disconnected `Peripheral` or a `LittleBluetoothError`
    @discardableResult
    public func disconnect() -> AnyPublisher<Peripheral, LittleBluetoothError> {
        
        guard let periph = self.peripheral else {
            return Result<Peripheral, LittleBluetoothError>.Publisher(.failure(.peripheralNotConnectedOrAlreadyDisconnected)).eraseToAnyPublisher()
        }
        
        let disconnectionSubject = PassthroughSubject<Peripheral, LittleBluetoothError>()
        let key = UUID()
        
        self.centralProxy.connectionEventPublisher
        .log(self, "Disconnecting", .debug, .connection)
        .filter{ (event) -> Bool in
            if case ConnectionEvent.disconnected(_, error: _) = event {
                return true
            }
            return false
        }
        .sink { [unowned self, key, periph] (event) in
            if case ConnectionEvent.disconnected( _, let error) = event {
                if error != nil {
                    disconnectionSubject.send(completion: .failure(error!))
                } else {
                    disconnectionSubject.send(periph)
                    disconnectionSubject.send(completion: .finished)
                }
                self.removeAndCancelSubscriber(for: key)
                // Everything is cleaned in the connection event observer
            }
        }
        .store(in: disposeBag, for: key)
        
        self.cbCentral.cancelPeripheralConnection(periph.cbPeripheral)
        return disconnectionSubject.eraseToAnyPublisher()
    }
    
    // MARK: - Extraction and restart
    /// Sometimes you may need to extract `CBCentralManager` and `CBPeripheral`
    /// During this operation everything is stopped, delegates are set to nil current operation cancelled
    /// - returns: A tuple with the central and the peripheral if connected
    public func extract() -> (central: CBCentralManager, peripheral: CBPeripheral?) {
        let cbCentral = self.cbCentral
        let cbPeripheral = self.peripheral?.cbPeripheral
        cbCentral.delegate = nil
        cbPeripheral?.delegate = nil
        // Clean operation
        cleanUpForExtraction()
        return (central: cbCentral, peripheral: cbPeripheral)
    }
    
    public func restart(with central: CBCentralManager, peripheral: CBPeripheral? = nil) {
        cbCentral = central
        cbCentral.delegate = centralProxy
        if let periph = peripheral {
            self.peripheral = Peripheral(periph)
            listenPublisherCancellable = _listenPublisher.connect()
            peripheralStatePublisherCancellable = _peripheralStatePublisher.connect()
            peripheralChangesPublisherCancellable = _peripheralChangesPublisher.connect()
        }
        
    }
    
    // MARK: - Discover Services and characteristics
    
    /// Discover services exposed from a connected `peripheral`
    /// - Parameter service: A service identifier. If `nil` a full scan of services exposed is made
    /// - Returns: And array of `CBService`
    public func discover(_ service: LittleBlueToothServiceIndentifier?) -> AnyPublisher<[CBService]?, LittleBluetoothError> {
        let discoverSubject = PassthroughSubject<[CBService]?, LittleBluetoothError>()
        let futKey = UUID()
        ensureBluetoothState()
            .log(self, "Discovering services", .debug, .gatt)
            .flatMap { [unowned self] _ in
                self.emit("discover: delegate=\(self.peripheral?.cbPeripheral.delegate != nil)", .trace, .gatt)
                guard let peripheral = self.peripheral else {
                    return Fail<[CBService]?, LittleBluetoothError>(error: LittleBluetoothError.peripheralNotFound)
                        .eraseToAnyPublisher()
                }
                return peripheral.getService(serviceUUID: service !=  nil ? CBUUID(string: service!) : nil )
            }
            .sink(receiveCompletion: { [unowned self, futKey] (completion) in
                switch completion {
                case .finished:
                    break
                case .failure(let error):
                    discoverSubject.send(completion: .failure(error))
                    self.removeAndCancelSubscriber(for: futKey)
                }
            }) { [unowned self, futKey] (readvalue) in
                discoverSubject.send(readvalue)
                discoverSubject.send(completion: .finished)
                self.removeAndCancelSubscriber(for: futKey)
            }
            .store(in: disposeBag, for: futKey)
        return discoverSubject.eraseToAnyPublisher()
    }
    
    
    /// Discover  characteristics from a specific service
    /// - Parameters:
    ///   - characteristic: A characteristic identifier. If `nil` a full scan of characteristic exposed by that service is made
    ///   - service: the `CBService` instance of the characteristic searched
    /// - Returns: A an array of  `CBCharacteristic`
    public func discover(_ characteristic: LittleBlueToothCharacteristicIndentifier?,
                         from service: CBService) -> AnyPublisher<[CBCharacteristic]?, LittleBluetoothError> {
        let discoverSubject = PassthroughSubject<[CBCharacteristic]?, LittleBluetoothError>()
        let futKey = UUID()
        ensureBluetoothState()
            .log(self, "Discovering characteristics", .debug, .gatt)
            .flatMap { [unowned self] _ in
                guard let peripheral = self.peripheral else {
                    return Fail<[CBCharacteristic]?, LittleBluetoothError>(error: LittleBluetoothError.peripheralNotFound)
                        .eraseToAnyPublisher()
                }
                return peripheral.discoverCharacteristics(characteristic != nil ? [CBUUID(string: characteristic!)] : nil, fromService: service.uuid)
                    .map { $0 as [CBCharacteristic]? }
                    .eraseToAnyPublisher()
            }
            .sink(receiveCompletion: { [unowned self, futKey] (completion) in
                switch completion {
                case .finished:
                    break
                case .failure(let error):
                    discoverSubject.send(completion: .failure(error))
                    self.removeAndCancelSubscriber(for: futKey)
                }
            }) { [unowned self, futKey] (characteristics) in
                discoverSubject.send(characteristics)
                discoverSubject.send(completion: .finished)
                self.removeAndCancelSubscriber(for: futKey)
            }
            .store(in: disposeBag, for: futKey)
        return discoverSubject.eraseToAnyPublisher()
    }
    
    #if !TEST
    /// Open an L2CAP channel to the connected peripheral
    /// - parameter psm: The Protocol/Service Multiplexer (PSM) value for the channel
    /// - returns: A publisher with the opened L2CAP channel or a LittleBluetoothError
    public func openL2CAPChannel(psm: CBL2CAPPSM) -> AnyPublisher<CBL2CAPChannel, LittleBluetoothError> {

        let l2capSubject = PassthroughSubject<CBL2CAPChannel, LittleBluetoothError>()
        let key = UUID()

        ensureBluetoothState()
            .log(self, "Opening L2CAP channel", .debug, .l2cap)
            .flatMap { [unowned self] _ in
                self.ensurePeripheralReady()
            }
            .flatMap { periph in
                periph.openL2CAPChannel(psm: psm)
            }
            .sink(receiveCompletion: { [unowned self, key] (completion) in
                switch completion {
                case .finished:
                    break
                case .failure(let error):
                    l2capSubject.send(completion: .failure(error))
                    self.removeAndCancelSubscriber(for: key)
                }
            }) { [unowned self, key] (channel) in
                l2capSubject.send(channel)
                l2capSubject.send(completion: .finished)
                self.removeAndCancelSubscriber(for: key)
            }
            .store(in: disposeBag, for: key)

        return l2capSubject.eraseToAnyPublisher()
    }
    #endif
    
    // MARK: - Private
    private func restore(_ restorer: CentralRestorer) -> Restored {
          // Restore scan if scanning
          if restorer.centralManager.isScanning {
              let restoreDiscoverServices = restorer.services
              let restoreScanOptions = restorer.scanOptions
              let restoreDiscoveryPublisher = self.startDiscovery(withServices: restoreDiscoverServices, options: restoreScanOptions)
              emit("Scan restore, isScanning: \(restorer.centralManager.isScanning)", .info, .restore)
              return .scan(discoveryPublisher: restoreDiscoveryPublisher)
          }
          if let cbPeripheral = restorer.peripherals.first {
              self.restoredCBPeripheral = cbPeripheral
              self.peripheral = Peripheral(cbPeripheral)
              self.peripheral!.skipServiceCache = true
              switch cbPeripheral.state {
              case .connected:
                  // When peripheral is restored in connected state, we need to trigger the connection event flow
                  // since didConnect delegate method won't be called for an already-connected peripheral
                  self.peripheralChangesPublisherCancellable = self._peripheralChangesPublisher.connect()
                  self.connectPeripheralStatePublisher()
                  self.listenPublisherCancellable = self._listenPublisher.connect()
                  emit("Restore: peripheral already .connected — triggering autoConnected event", .info, .restore)

                  // Send the autoConnected event to trigger the normal connection flow (connectionTasks -> ready)
                  // This must happen async to allow subscribers to set up first
                  DispatchQueue.main.async { [weak self] in
                      guard let self = self else { return }
                      self.centralProxy.connectionEventPublisher.send(.autoConnected(cbPeripheral))
                  }
              case .connecting:
                  self.peripheralChangesPublisherCancellable = self._peripheralChangesPublisher.connect()
                  self.connectPeripheralStatePublisher()
                  emit("Restore: peripheral is .connecting — will re-issue connect after poweredOn", .info, .restore)
                  // Wait for poweredOn before re-issuing connect
                  self.centralProxy.centralStatePublisher
                      .first(where: { $0 == .poweredOn })
                      .sink { [weak self] _ in
                          guard let self = self else { return }
                          self.emit("Restore: poweredOn received — re-issuing connect", .info, .restore)
                          self.centralProxy.isAutoconnectionActive = true
                          self.cbCentral.connect(cbPeripheral, options: nil)
                      }
                      .store(in: self.disposeBag, for: UUID())
              case .disconnected:
                  self.connectPeripheralStatePublisher()
                  emit("Restore: peripheral is .disconnected — autoconnection handler will fire if set", .info, .restore)
              case .disconnecting:
                  self.connectPeripheralStatePublisher()
                  emit("Restore: peripheral is .disconnecting", .info, .restore)
              @unknown default:
                  fatalError("Connection event in default not handled")
              }
              emit("Peripheral restore: \(cbPeripheral.identifier), state: \(cbPeripheral.state.rawValue), hasDelegate: \(cbPeripheral.delegate != nil)", .info, .restore)
              return Restored.peripheral(self.peripheral!)
          }
          return Restored.nothing
      }
    
    private func ensureBluetoothState() -> AnyPublisher<BluetoothState, LittleBluetoothError> {

        let futKey = UUID()
        let future = Deferred {
            Future<BluetoothState, LittleBluetoothError> { [unowned self] prom in
                self.centralProxy.centralStatePublisher
                .log(self, "Checking central state", .trace, .connection)
                .tryFilter { [unowned self] (state) -> Bool in
                    switch state {
                    case .poweredOff:
                        if let periph = self.peripheral {
                            let connEvent = ConnectionEvent.disconnected(periph.cbPeripheral, error: .bluetoothPoweredOff)
                            self.centralProxy.connectionEventPublisher.send(connEvent)
                        }
                        throw LittleBluetoothError.bluetoothPoweredOff
                    case .unauthorized:
                        throw LittleBluetoothError.bluetoothUnauthorized
                    case .unsupported:
                        throw LittleBluetoothError.bluetoothUnsupported
                    case .unknown, .resetting:
                        return false
                    case .poweredOn:
                        return true
                    }
                }
                .mapError { (error) -> LittleBluetoothError in
                    error as! LittleBluetoothError
                }
                .map { state -> BluetoothState in
//                    print("CBManager state: \(state)")
                    return state
                }
                .sink(receiveCompletion: { [unowned self, futKey] (completion) in
                    switch completion {
                    case .finished:
                        break
                    case .failure(let error):
                        prom(.failure(error))
                        self.removeAndCancelSubscriber(for: futKey)
                    }
                }) { [unowned self, futKey] (state) in
                    prom(.success(state))
                    self.removeAndCancelSubscriber(for: futKey)
                }
                .store(in: self.disposeBag, for: futKey)
            }
        }
        return future.eraseToAnyPublisher()

    }
   
    private func ensurePeripheralReady() -> AnyPublisher<Peripheral, LittleBluetoothError> {
        emit("ensurePeripheralReady: peripheral=\(peripheral?.cbPeripheral.identifier.uuidString ?? "nil"), state=\(peripheral?.state ?? .disconnected), delegate=\(peripheral?.cbPeripheral.delegate != nil)", .trace, .connection)
        guard let periph = peripheral, periph.state == .connected else {
            let state = peripheral?.state
            return Result<Peripheral, LittleBluetoothError>.Publisher(.failure(.peripheralNotConnected(state: state ?? .disconnected))).eraseToAnyPublisher()
        }
        
        return self.centralProxy.connectionEventPublisher
        .log(self, "Ensuring peripheral ready", .trace, .connection)
        .tryFilter { (event) -> Bool in
            switch event {
            case .disconnected(_, let error?):
                throw error
            case .disconnected(let periph, _):
                throw LittleBluetoothError.peripheralDisconnected(PeripheralIdentifier(peripheral: periph), nil)
            case .autoConnected(_),
                 .connected(_),
                 .connectionFailed(_, _),
                 .notReady(_, _):
                return false
            case .ready(_):
                return true
            }
        }
        .tryMap { [unowned self] (_) -> Peripheral in
            guard let p = self.peripheral else {
                throw LittleBluetoothError.peripheralNotConnected(state: .disconnected)
            }
            return p
        }
        .mapError { (error) -> LittleBluetoothError in
            error as! LittleBluetoothError
        }
        .merge(with: Just(periph).setFailureType(to: LittleBluetoothError.self))

        .eraseToAnyPublisher()
    }
    
    private func removeAndCancelSubscriber(for key: UUID) {
        disposeBag.remove(key)
    }
    
    func cleanUpForDisconnection() {
        emit("cleanUpForDisconnection", .trace, .connection)
        if cbCentral.isScanning {
            cbCentral.stopScan()
            scanning?.cancel()
            scanning = nil
        }
        listenPublisherCancellable?.cancel()
        listenPublisherCancellable = nil
        _listenPublisher_ = nil
        peripheralStatePublisherCancellable?.cancel()
        peripheralStatePublisherCancellable = nil
        peripheralStatePipeCancellable?.cancel()
        peripheralStatePipeCancellable = nil
        _peripheralStatePublisher_ = nil
        peripheralChangesPublisherCancellable?.cancel()
        peripheralChangesPublisherCancellable = nil
        _peripheralChangesPublisher_ = nil
        peripheral = nil
    }

    private func cleanUpForExtraction() {
        cbCentral.stopScan()
        scanning?.cancel()
        scanning = nil
        cleanUpForDisconnection()
    }
    
}

