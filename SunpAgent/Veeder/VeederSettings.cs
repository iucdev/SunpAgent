using System.IO.Ports;
using static SunpAgent.VeederClient;

namespace SunpAgent.Veeder;
internal class VeederSettings {
    public string DComPwd { get; set; } = string.Empty;
    public ConnectionType ConnectionType { get; set; }

    public string PortName { get; set; } = string.Empty;
    public int? BaudRate { get; set; }
    public int? DataBits { get; set; }
    public Parity Parity { get; set; }
    public StopBits StopBits { get; set; }
    public int? ReadTimeout { get; set; }
    public int? WriteTimeout { get; set; }

    public string IpAddress { get; set; } = string.Empty;
    public int? Port { get; set; }
    public int IsLogTransmission { get; set; }

    public long TankId { get; set; }
    public string NameConst { get; set; } = string.Empty; //Имя/номер источника (резервуара, расходомера) как константа
}
