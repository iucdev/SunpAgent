using System.Diagnostics;
using System.Globalization;
using System.IO.Ports;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using SunpAgent.Veeder;

namespace SunpAgent;
internal class VeederClient {
    private readonly ILogger<VeederClient> _logger;
    private bool _isEnableLog;

    private int _lastCom;
    private int _cmdDelay = 1000;//задержка для команды в мс.
    private const int _receiveBufferSize = 131072;//65536;
    private byte[] _buf = new byte[_receiveBufferSize];
    private byte[] _cmdBuf = new byte[256];
    private int _timeoutCntr = 0;

    private Socket? _socket; //Socket
    private SerialPort? _sPort; //COM Port
    private VeederSettings _settings;
    private readonly CancellationToken _cancellationToken;

    private List<SourceValue> _sourceValues = new List<SourceValue>();
    private List<SourceValue> _sourceValuesTemp = new List<SourceValue>();
    private List<TransferDTO> _transferDtos = new List<TransferDTO>();
    private List<TransferDTO> _transferDtosTemp = new List<TransferDTO>();

    public VeederClient(ILogger<VeederClient> logger, VeederSettings settings, CancellationToken cancellationToken = default) {
        _logger = logger;
        _cancellationToken = cancellationToken;
        _settings = settings;
    }

    public Task Start(Action<VedeerClientActionResult> callback, TimeSpan repeat) {
        init();

        var task = PeriodicTask
            .Run(async () => {
                Stopwatch stopwatch = Stopwatch.StartNew();

                _sourceValues = new List<SourceValue>();
                _sourceValuesTemp = new List<SourceValue>();
                _transferDtos = new List<TransferDTO>();
                _transferDtosTemp = new List<TransferDTO>();

                if (_cancellationToken.IsCancellationRequested) {
                    StopCollection();
                    return;
                }

                _logger.LogInformation("Timer");

                if (GetConnectionState()) {
                    try {
                        getData();
                    } catch (Exception ex) {
                        _logger.LogError("RefreshTags error {0}", ex.Message);
                        _logger.LogError("RefreshTags error {0}", ex.StackTrace);
                    }

                    try {
                        await Task.Delay(2000, _cancellationToken);
                    } catch (OperationCanceledException) {
                        return;
                    } catch (Exception) {
                        Thread.Sleep(2000);
                    }
                    if (_cancellationToken.IsCancellationRequested) {
                        return;
                    }
                } else {
                    _logger.LogError("Нет связи с сервером источника данных!");
                    StopCollection();

                    await Task.Delay(30000, _cancellationToken);
                    if (_cancellationToken.IsCancellationRequested) {
                        return;
                    }

                    if (!_cancellationToken.IsCancellationRequested) {
                        reconnect();
                    }
                }

                stopwatch.Stop();
                callback(new VedeerClientActionResult {
                    SourceValues = _sourceValues,
                    TransferDtos = _transferDtos,
                    CompletionTime = stopwatch.Elapsed
                });

                if (!_cancellationToken.IsCancellationRequested) {
                    return;
                } else {
                    StopCollection();
                }
            }, repeat, _cancellationToken, false);

        return task;
    }

    public void Stop() {
        CancellationTokenSource source = new CancellationTokenSource();
        CancellationToken token = _cancellationToken;
        source.Cancel();
    }

    private void init() {
        _logger.LogDebug("Init");

        //Thread.CurrentThread.CurrentUICulture = new CultureInfo("en-US");
        //Thread.CurrentThread.CurrentCulture = new CultureInfo("en-US");
        _isEnableLog = _settings.IsLogTransmission == 1;

        try {
            var counter = 0;
            while (!Connect()) {
                if (_cancellationToken.IsCancellationRequested) {
                    return;
                }

                _logger.LogInformation("Try connect to server {0}", counter++);
                Task.Delay(60000, _cancellationToken);
                if (_cancellationToken.IsCancellationRequested) {
                    return;
                }
            }
        } catch (Exception e) {
            _logger.LogError(e.Message, e);
        }
    }

    protected void reconnect() {
        _logger.LogError("Reconnecting...");
        init();
    }

    private void getData() {
        if (_cancellationToken.IsCancellationRequested) {
            return;
        }

        if (string.IsNullOrEmpty(_settings.NameConst)) {
            throw new Exception($"Unable to find _settings.NameConst for TankId {_settings.TankId}");
        }

        if (int.TryParse(_settings.NameConst, out int nom)) {
            if (Send231(nom, _settings.DComPwd) <= 0) {
                _logger.LogDebug("F231 return 0");
            }
        }

        if (_cancellationToken.IsCancellationRequested) {
            return;
        }

        if (_sourceValuesTemp.Any()) {
            foreach (var sourceValue in _sourceValuesTemp) {
                if (_cancellationToken.IsCancellationRequested) {
                    return;
                }

                sourceValue.TankId = _settings.TankId;
            }

            _sourceValues.AddRange(_sourceValuesTemp);
        }

        try {
            Task.Delay(500, _cancellationToken);
        } catch (OperationCanceledException) {
            return;
        } catch (Exception) {
            Thread.Sleep(500);
        }

        if (_cancellationToken.IsCancellationRequested) {
            return;
        }

        if (Send215(nom, _settings.DComPwd) <= 0) {
            _logger.LogDebug("F215 return 0");
        }

        if (_transferDtosTemp.Any()) {
            foreach (var transferItem in _transferDtosTemp) {
                if (_cancellationToken.IsCancellationRequested) {
                    return;
                }
                transferItem.TankId = _settings.TankId;
            }
            _transferDtos.AddRange(_transferDtosTemp);
        }
    }

    private int Send231(int nom, string pass) {
        if (string.IsNullOrEmpty(pass))
            return Send231NP(nom);

        _lastCom = VrCMD.CMD231;
        _cmdDelay = 4000;
        _cmdBuf[0] = VrCMD.SOH;
        _cmdBuf[1] = (byte)pass[0];
        _cmdBuf[2] = (byte)pass[1];
        _cmdBuf[3] = (byte)pass[2];
        _cmdBuf[4] = (byte)pass[3];
        _cmdBuf[5] = (byte)pass[4];
        _cmdBuf[6] = (byte)pass[5];
        _cmdBuf[7] = 105;
        _cmdBuf[8] = 48 + 2;
        _cmdBuf[9] = 48 + 3;
        _cmdBuf[10] = 48 + 1;
        _cmdBuf[11] = 48 + 0;
        _cmdBuf[12] = (byte)(48 + nom);
        _logger.LogInformation("F231P");
        return SendBuf(13);
    }

    private int Send231NP(int nom) {
        _lastCom = VrCMD.CMD231;
        _cmdDelay = 4000;
        _cmdBuf[0] = VrCMD.SOH;
        _cmdBuf[1] = 105;
        _cmdBuf[2] = 48 + 2;
        _cmdBuf[3] = 48 + 3;
        _cmdBuf[4] = 48 + 1;
        _cmdBuf[5] = 48 + 0;
        _cmdBuf[6] = (byte)(48 + nom);
        _logger.LogInformation("F231");
        return SendBuf(7);
    }

    private int Send215(int nom, string pass) {
        if (string.IsNullOrEmpty(pass))
            return Send215NP(nom);

        _lastCom = VrCMD.CMD215;
        _cmdDelay = 8000;//2000;
        _cmdBuf[0] = VrCMD.SOH;
        _cmdBuf[1] = (byte)pass[0];
        _cmdBuf[2] = (byte)pass[1];
        _cmdBuf[3] = (byte)pass[2];
        _cmdBuf[4] = (byte)pass[3];
        _cmdBuf[5] = (byte)pass[4];
        _cmdBuf[6] = (byte)pass[5];
        _cmdBuf[7] = 105;
        _cmdBuf[8] = 48 + 2;
        _cmdBuf[9] = 48 + 1;
        _cmdBuf[10] = 48 + 5;
        _cmdBuf[11] = 48 + 0;
        _cmdBuf[12] = (byte)(48 + nom);
        _logger.LogInformation("F215P");
        return SendBuf(13);
    }

    /// <summary>
    /// In-Tank Mass/Density Delivery Report
    /// </summary>
    /// <param name="nom"></param>
    /// <returns></returns>
    private int Send215NP(int nom) {
        _lastCom = VrCMD.CMD215;
        _cmdDelay = 8000;//2000;
        _cmdBuf[0] = VrCMD.SOH;
        _cmdBuf[1] = 105;
        _cmdBuf[2] = 48 + 2;
        _cmdBuf[3] = 48 + 1;
        _cmdBuf[4] = 48 + 5;
        _cmdBuf[5] = 48 + 0;
        _cmdBuf[6] = (byte)(48 + nom);
        _logger.LogInformation("F215");
        return SendBuf(7);
    }

    private bool SkipCmd() {
        return _lastCom == VrCMD.ESC;
    }

    private bool CheckRb(int bytesRec) {
        if (_buf[bytesRec - 1] == 0x03)
            return true;
        _logger.LogError("Error check summ");
        return false;
    }

    private bool Fill(int bytesRec) {
        var result = false;

        switch (_lastCom) {
            case VrCMD.CMD231: {
                _sourceValuesTemp = Fill231();
                if (_sourceValuesTemp != null && _sourceValuesTemp.Count > 0)
                    result = true;
                break;
            }
            case VrCMD.CMD201: {
                _sourceValuesTemp = Fill201();
                if (_sourceValuesTemp != null && _sourceValuesTemp.Count > 0)
                    result = true;
                break;
            }
            case VrCMD.CMD215: {
                _transferDtosTemp = Fill215();
                if (_transferDtosTemp != null && _transferDtosTemp.Count > 0)
                    result = true;
                break;
            }
        }
        return result;
    }

    private List<SourceValue> Fill231() {
        var sourceValues = new List<SourceValue>();

        try {
            if (!((_buf[0] == 01) && (_buf[1] == 0x69) && (_buf[2] == 0x32) && (_buf[3] == 0x33) && (_buf[4] == 0x31))) {
                _logger.LogError("Error F231");
                return sourceValues;
            }

            var dt = fillDTVR(7);
            var pos = 17;

            while (_buf[pos] != 0x26) {
                var sourceValue = new SourceValue { DatetimeStamp = dt };
                //Logger.InfoFormat(Environment.NewLine);
                //Logger.InfoFormat("dt={0}", dt);
                var tanknum = GetValue(pos, 2);
                sourceValue.Name = tanknum.ToString(CultureInfo.InvariantCulture);
                //Logger.InfoFormat("tank={0} p={1}; s1={2}; s2={3}; s3={4}; s4={5};", tanknum, buf[pos + 2], buf[pos + 3], buf[pos + 4], buf[pos + 5], buf[pos + 6]);

                var n = GetValue(pos + 7, 2);
                pos += 9;

                //Logger.InfoFormat("N={0} Pos={1}", n, pos);
                for (var i = 0; i < n; i++) {
                    float si;
                    try {
                        si = Tofloat(pos);
                    } catch (Exception e) {
                        _logger.LogError(e.Message, e);
                        return sourceValues;
                    }

                    pos += 8;
                    var result = si.ToString("F4").Replace(',', '.');

                    switch (i) {
                        case 0:
                            sourceValue.Volume = result;
                            break;
                        case 3:
                            sourceValue.Level = result;
                            break;
                        case 5:
                            sourceValue.Temperature = result;
                            break;
                        case 10:
                            sourceValue.Mass = result;
                            break;
                        case 11:
                            sourceValue.Density = result;
                            break;
                    }
                }

                //Logger.InfoFormat("Vol={0} Lev={1}; Temp={2}; Mass={3}; Dens={4}; ", sourceValue.Volume, sourceValue.Level, sourceValue.Temperature, sourceValue.Mass, sourceValue.Density);
                sourceValues.Add(sourceValue);
            }
        } catch (Exception e) {
            _logger.LogError(e.Message, e);
        }

        return sourceValues;
    }

    private List<TransferDTO> Fill215() {
        var label = new[]
        {
                "SVol", "SMass", "SDens", "SWat", "STemp", "EVol", "EMass", "EDens", "EWat", "ETemp", "SHeight", "EHeight", "STcDens",
                "ETcDens", "STcVol", "ETcVol", "STcOffs", "ETcOff"
            };

        var transferDtos = new List<TransferDTO>();

        try {
            if (!((_buf[0] == 01) && (_buf[1] == 0x69) && (_buf[2] == 0x32) && (_buf[3] == 0x31) && (_buf[4] == 0x35))) {
                _logger.LogError("Error F215");
                return transferDtos;
            }

            var dt = fillDTVR(7);
            var pos = 17;
            //Logger.Info("dt={0}", dt);

            while (_buf[pos] != 0x26) {

                var tt = GetValue(pos, 2);
                var p = _buf[pos + 2];
                var dd = GetValue(pos + 3, 2);
                pos += 5;
                //Logger.Info("tt={0}; p={1}; dd={2}", tt, p, dd);

                //Logger.Info("sDate      | eDate     {0}| f   |", string.Join("", label.Select(s => string.Format("| {0,-10}", s)).ToArray()));

                for (var i = 0; i < dd; i++) {
                    var value = new TransferDTO();

                    var dt1 = fillDTVR(pos);
                    pos += 10;

                    var dt2 = fillDTVR(pos);
                    pos += 10;

                    value.StartTime = dt1 != null ? dt1.Value : DateTime.MinValue;
                    value.EndTime = dt2 != null ? dt2.Value : DateTime.MinValue;

                    var line = string.Format("{0,-11}| {1,-10}", value.StartTime, value.EndTime);

                    float sVol = 0, eVol = 0, sMass = 0, eMass = 0;

                    var nn = GetValue(pos, 2);
                    nn = int.Parse(nn.ToString(CultureInfo.InvariantCulture), NumberStyles.HexNumber);
                    pos += 2;

                    for (var j = 0; j < nn; j++) {
                        float si;
                        try {
                            si = Tofloat(pos);
                        } catch (Exception e) {
                            _logger.LogError(e.Message, e);
                            return transferDtos;
                        }
                        pos += 8;
                        line += string.Format("| {0,-10}", si);

                        switch (j) {
                            case 0:
                                sVol = si;
                                break;
                            case 1:
                                sMass = si;
                                break;
                            case 5:
                                eVol = si;
                                break;
                            case 6:
                                eMass = si;
                                break;
                            case 7:
                                value.Density = si;
                                break;
                            case 11:
                                value.Level = si;
                                break;
                        }
                    }

                    value.Volume = eVol - sVol;
                    value.Mass = eMass - sMass;

                    line += string.Format("| {0,5}", _buf[pos]);
                    //Logger.Info("{0}", line);

                    pos++; //f

                    transferDtos.Add(value);
                }
                pos++; //f
            }
        } catch (Exception e) {
            _logger.LogError(e.Message, e);
            return transferDtos;
        }

        return transferDtos;
    }

    private List<SourceValue> Fill201() {
        var sourceValues = new List<SourceValue>();

        try {
            if (!((_buf[0] == 01) && (_buf[1] == 0x69) && (_buf[2] == 0x32) && (_buf[3] == 0x30) && (_buf[4] == 0x31))) {
                _logger.LogError("Error F201");
                return sourceValues;
            }

            var dt = fillDTVR(7);
            var pos = 17;

            while (_buf[pos] != 0x26) {
                var sourceValue = new SourceValue { DatetimeStamp = dt };
                var tanknum = GetValue(pos, 2);
                sourceValue.Name = tanknum.ToString(CultureInfo.InvariantCulture);
                var n = GetValue(pos + 7, 2);
                pos += 9;

                for (var i = 0; i < n; i++) {
                    float si;
                    try {
                        si = Tofloat(pos);
                    } catch (Exception e) {
                        _logger.LogError(e.Message, e);
                        return sourceValues;
                    }

                    pos += 8;
                    var result = si.ToString("F4").Replace(',', '.');

                    switch (i) {
                        case 0:
                            sourceValue.Volume = result;
                            break;
                        case 3:
                            sourceValue.Level = result;
                            break;
                        case 5:
                            sourceValue.Temperature = result;
                            break;
                        case 10:
                            sourceValue.Mass = result;
                            break;
                        case 11:
                            sourceValue.Density = result;
                            break;
                    }
                }

                //Logger.InfoFormat("Vol={0} Lev={1}; Temp={2}; Mass={3}; Dens={4}; ", sourceValue.Volume, sourceValue.Level, sourceValue.Temperature, sourceValue.Mass, sourceValue.Density);
                sourceValues.Add(sourceValue);
            }
        } catch (Exception e) {
            _logger.LogError(e.Message, e);
        }

        return sourceValues;
    }

    protected int SendBuf(int len) {
        switch (_settings.ConnectionType) {
            case ConnectionType.Com:
                return SendBufCom(len);// COM
            case ConnectionType.Ip:
                return SendBufIp(len);  // IP
            default:
                throw new NotImplementedException("Unknown ConnectionType!");
        }
    }

    protected bool Connect() {
        switch (_settings.ConnectionType) {
            case ConnectionType.Com:
                return ConnectCom();// Коннект к удаленному устройству COM
            case ConnectionType.Ip:
                return ConnectIp();  // Коннект к удаленному устройству IP
            default:
                throw new Exception("Unknown ConnectionType!");
        }
    }

    private bool ConnectCom() {
        try {
            _sPort = new SerialPort {
                PortName = _settings.PortName,
                BaudRate = _settings.BaudRate != null ? _settings.BaudRate.Value : 0,
                DataBits = _settings.DataBits != null ? _settings.DataBits.Value : 0,
                Parity = _settings.Parity,
                StopBits = _settings.StopBits,
                ReadTimeout = _settings.ReadTimeout != null ? _settings.ReadTimeout.Value : 0,
                WriteTimeout = _settings.WriteTimeout != null ? _settings.WriteTimeout.Value : 0
            };
            // настройки порта
            _sPort.Open();
            _logger.LogInformation("Client connected to {0}({2}) State {1}", _sPort.PortName, _sPort.IsOpen, string.Format("{0}, {1}, {2}, {3}, {4}, {5}", _sPort.BaudRate, _sPort.DataBits, _sPort.Parity, _sPort.StopBits, _sPort.ReadTimeout, _sPort.WriteTimeout));

        } catch (Exception e) {
            _logger.LogError("ERROR: невозможно открыть порт:{0} {1}", _settings.PortName, e);
            return false;
        }
        return true;
    }

    private bool ConnectIp() {
        // Коннектим носок к удаленному устройству. Ловим ошибки
        try {

            //var ipHostInfo = Dns.GetHostEntry(_settings.IpAddress);// Dns.Resolve(_settings.IpAddress);
            var ipHostInfo = Dns.Resolve(_settings.IpAddress);
            //Logger.Debug("AdrL {0}",ipHostInfo.AddressList.Length);
            //Logger.Debug("Adr0 {0}", ipHostInfo.AddressList[0]);
            var ipAddress = ipHostInfo.AddressList[0];

            if (!_settings.Port.HasValue || !Regex.IsMatch(_settings.Port.Value.ToString(), @"^\d{1,5}$", RegexOptions.None)) {
                return false;
            }
            var port = _settings.Port.Value.ToString();
            var ep = new IPEndPoint(ipAddress, int.Parse(port));

            // Создаем TCP/IP  сокет
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            //_socket = new Socket(ep.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            //_socket.ReceiveTimeout = 500;//fake
            //_socket.SendTimeout = 500;//fake

            _socket.Connect(ep);
            _logger.LogInformation("Socket connected to {0}", _socket.RemoteEndPoint);
        } catch (ArgumentNullException ane) {
            _logger.LogError("ArgumentNullException : {0}", ane);
            return false;
        } catch (SocketException se) {
            _logger.LogError("{0}:{1} SocketException : {2}", _settings.IpAddress, _settings.Port!.Value, se);
            return false;
        } catch (Exception e) {
            _logger.LogError("Unexpected exception : {0}", e);
            return false;
        }

        Thread.Sleep(1000);
        return true;
    }

    private int SendBufCom(int len) {
        if (_isEnableLog)
            _logger.LogDebug("Sent {0} byte->{1}", len, BitConverter.ToString(_cmdBuf, 0, len).Replace('-', ' '));

        try {
            // Шлем запрос
            if (_sPort != null && _sPort.IsOpen)
                _sPort.Write(_cmdBuf, 0, len);
            else
                return 0;
        } catch (TimeoutException) {
            _logger.LogDebug("Write Timeout");
            return 0;
        } catch (Exception) {
            return 0;
        }

        if (SkipCmd())
            return 0;

        Thread.Sleep(_cmdDelay);
        // Получаем ответ с удаленного устройства
        var bytesRec = 0;
        try {
            bytesRec = _sPort.Read(_buf, 0, _receiveBufferSize);
        } catch (TimeoutException) {
            _logger.LogDebug("Read Timeout");
            return 0;
        } catch (Exception) {
            return 0;
        }
        if (_isEnableLog)
            _logger.LogDebug("Receive {0} byte <- {1}", bytesRec, BitConverter.ToString(_buf, 0, bytesRec).Replace('-', ' '));

        if (!CheckRb(bytesRec))
            return 0;

        return !Fill(bytesRec) ? 0 : bytesRec;
    }

    private int SendBufIp(int len) {
        //IsEnableLog = true;//раскоментить при дебаге когда нет манагера 

        if (_isEnableLog)
            _logger.LogDebug("Sent {0} byte->{1}", len, BitConverter.ToString(_cmdBuf, 0, len).Replace('-', ' '));

        // Шлем запрос
        _socket.Send(_cmdBuf, 0, len, SocketFlags.None);

        if (SkipCmd())
            return 0;

        Thread.Sleep(_cmdDelay);

        //if (_socket.Available <= 0) return 0;
        if (!SocketExtensions.IsConnected(_socket)) {
            _logger.LogInformation("sa0");
            return 0;
        }

        // Получаем ответ с удаленного устройства
        //var bytesRec = _socket.Receive(buf, ReceiveBufferSize, SocketFlags.None);

        int bytesRec = 0;
        int readTimeOut = 1000000;

        if (_settings.ReadTimeout.HasValue)
            readTimeOut = _settings.ReadTimeout.Value * 1000000;

        //set time out = 10 sec.
        if (_socket.Poll(readTimeOut, SelectMode.SelectRead)) {
            bytesRec = _socket.Receive(_buf, _receiveBufferSize, SocketFlags.None);
            _timeoutCntr = 0;
        } else {
            _logger.LogError("Receive timeout"); // Timed out
            _timeoutCntr++;

            if (_timeoutCntr >= 3)
                reconnect();
        }

        if (_isEnableLog)
            _logger.LogDebug("Receive {0} byte <- {1}", bytesRec, BitConverter.ToString(_buf, 0, bytesRec).Replace('-', ' '));

        if (bytesRec == 0 || !CheckRb(bytesRec))
            return 0;

        return !Fill(bytesRec) ? 0 : bytesRec;
    }

    protected bool StopCollection() {
        _logger.LogInformation("Stop collection");

        if (_settings.ConnectionType == ConnectionType.Ip && _socket != null && _socket.Connected) {
            _socket.Shutdown(SocketShutdown.Both);
            _socket.Close();
        }

        if (_settings.ConnectionType == ConnectionType.Com && _sPort != null && _sPort.IsOpen) {
            _sPort.Close();
        }

        return true;
    }

    /// <summary>
    /// Состояние связи с сервером
    /// </summary>
    /// <returns> связь есть-нет</returns>
    protected bool GetConnectionState() {
        try {
            if (_settings.ConnectionType == ConnectionType.Com)
                return _sPort != null && _sPort.IsOpen;
            if (_settings.ConnectionType == ConnectionType.Ip)
                return _socket != null && _socket.Connected && SocketExtensions.IsConnected(_socket);
        } catch (Exception e) {
            _logger.LogError(e.Message, e);
            return false;
        }
        return false;
    }

    protected int GetValue(int nom, int kol) {
        var result = 0;

        for (var i = 1; i <= kol; i++) {
            if (_buf[i - 1 + nom] <= 0x39)
                result = (int)Math.Round(result + (_buf[i - 1 + nom] - 0x30) * Math.Pow(10, kol - i));
            else
                result = (int)Math.Round(result + (_buf[i - 1 + nom] - 0x37) * Math.Pow(10, kol - i));
        }
        return result;
    }

    private DateTime? fillDTVR(int nom) {
        int mm, dd, yy, hh, mn;
        DateTime? result = null;
        try {
            yy = GetValue(nom, 2);
            mm = GetValue(nom + 2, 2);
            dd = GetValue(nom + 4, 2);
            hh = GetValue(nom + 6, 2);
            mn = GetValue(nom + 8, 2);
            result = new DateTime(yy + 2000, mm, dd, hh, mn, 0, 0);
        } catch (Exception e) {
            _logger.LogError("error on fillDTVR {0}", e);
        }
        return result;
    }

    private float Tofloat(int pos) {
        var raw = new byte[4];
        raw[3] = Convert.ToByte(((char)_buf[pos]).ToString(CultureInfo.InvariantCulture) + ((char)_buf[pos + 1]).ToString(CultureInfo.InvariantCulture), 16);
        raw[2] = Convert.ToByte(((char)_buf[pos + 2]).ToString(CultureInfo.InvariantCulture) + ((char)_buf[pos + 3]).ToString(CultureInfo.InvariantCulture), 16);
        raw[1] = Convert.ToByte(((char)_buf[pos + 4]).ToString(CultureInfo.InvariantCulture) + ((char)_buf[pos + 5]).ToString(CultureInfo.InvariantCulture), 16);
        raw[0] = Convert.ToByte(((char)_buf[pos + 6]).ToString(CultureInfo.InvariantCulture) + ((char)_buf[pos + 7]).ToString(CultureInfo.InvariantCulture), 16);

        return BitConverter.ToSingle(raw, 0);
    }

    static class SocketExtensions {
        public static bool IsConnected(Socket socket) {
            try {
                return !(socket.Poll(1, SelectMode.SelectRead) && socket.Available == 0);
            } catch (SocketException) {
                return false;
            }
        }
    }

    public class VrCMD {
        public const int SOH = 0x01;
        public const int ESC = 0x1B;
        public const int CMDStat = 0x4C;
        public const int CMDShift = 0x4;
        public const int CMD201 = 0x2010;
        public const int CMD202 = 0x2020;
        public const int CMD204 = 0x2040;
        public const int CMD205 = 0x2050;
        public const int CMD206 = 0x2060;
        public const int CMD214 = 0x2140;
        public const int CMD215 = 0x2150;
        public const int CMD231 = 0x2310;
        public const int CMD235 = 0x2350;
        public const int CMDA01 = 0xA010;
        public const int CMDA30 = 0xA300;
    }

    /// <summary>
    /// Тип соединения
    /// </summary>
    public enum ConnectionType : byte {
        UnDefind = 0,
        Ip = 1,
        Com = 2
    }

    public class SourceValue {
        public DateTime? DatetimeStamp { get; set; }
        public string Name { get; set; } = string.Empty;
        public long TankId { get; set; }
        public string Temperature { get; set; } = string.Empty;
        public string Density { get; set; } = string.Empty;
        public string Volume { get; set; } = string.Empty;
        public string Mass { get; set; } = string.Empty;
        public string Level { get; set; } = string.Empty;
    }

    public class TransferDTO {
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public double? Level { get; set; }
        public double? Volume { get; set; }
        public double? Mass { get; set; }
        public double? Density { get; set; }
        public long TankId { get; set; }
    }

    public static class PeriodicTask {
        public static async Task Run(
            Func<Task> action,
            TimeSpan period,
            CancellationToken cancellationToken = default(CancellationToken),
            bool takeIntoTaskCompletionTime = true) {
            while (!cancellationToken.IsCancellationRequested) {

                Stopwatch stopwatch = Stopwatch.StartNew();

                if (!cancellationToken.IsCancellationRequested)
                    await action();

                stopwatch.Stop();

                var delayTime = takeIntoTaskCompletionTime
                    ? period - stopwatch.Elapsed
                    : period;
                
                try {
                    await Task.Delay(period - stopwatch.Elapsed, cancellationToken);
                } catch (OperationCanceledException) {
                    return;
                } catch (Exception) {
                    Thread.Sleep(delayTime);
                }
            }
        }
    }

    public class VedeerClientActionResult {
        public List<SourceValue> SourceValues { get; set; } = new List<SourceValue>();
        public List<TransferDTO> TransferDtos { get; set; } = new List<TransferDTO>();
        public TimeSpan CompletionTime { get; set; }
    }
}
