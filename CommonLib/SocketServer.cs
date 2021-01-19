using System;
using System.Text;
using System.Net.Sockets;
using System.Net;
using System.Threading;

namespace CommonLib.SocketManager
{
    public delegate void SendDataHandle();

    public delegate void ReceiveDataHandle(Client client, byte[] data);

    public class SocketBase : System.Net.Sockets.Socket
    {
        public IPEndPoint address = null;
        public int psize = 1024;

        public SocketBase(SocketType sType, ProtocolType protocolType) :
            base(AddressFamily.InterNetwork, sType, protocolType)
        {
        }

        public void SetAddress(string ip, int port)
        {
            address = new IPEndPoint(IPAddress.Parse(ip), port);
        }

        public void SetPackageSize(int packageSize)
        {
            this.psize = packageSize;
        }

        public void Bind()
        {
            base.Bind(address);
        }
    }

    public class SocketServerBase : SocketBase
    {
        public SocketServerBase(SocketType sType, ProtocolType protocolType) : base(sType, protocolType)
        {
        }
    }

    public class Client
    {
        public IPEndPoint IP;
    }

    public class UdpServer : SocketServerBase
    {
        Thread thConn = null;
        public ReceiveDataHandle DataReceiveHandle;

        public UdpServer(string ip, int port): base(SocketType.Dgram, ProtocolType.Udp)
        {
            base.SetAddress(ip, port);
            base.Bind();
            this.SetOnData((Client client, byte[] data) =>
            {
                string message = Encoding.UTF8.GetString(data, 0, this.psize);
                Console.WriteLine("[Data] {0}", message);
            });
        }

        public void SetOnData(ReceiveDataHandle handle)
        {
            DataReceiveHandle = handle;
        }

        public void Start()
        {
            thConn = new Thread(() =>
            {
                OnConnectEvent();
            });
            thConn.IsBackground = true;
            thConn.Start();
        }

        public virtual void OnConnect(Client client, byte[] data, int length)
        {
            DataReceiveHandle(client, data);
        }

        private void OnConnectEvent()
        {
            EndPoint remote = new IPEndPoint(IPAddress.Any, 0);

            while (true)
            {
                byte[] data = new byte[this.psize];
                int length = base.ReceiveFrom(data, ref remote);

                Client client = new Client() {
                    IP = (IPEndPoint)remote
                };

                OnConnect(client, data, length);
            }
        }
    }

    public class UdpClient : SocketBase
    {
        Thread thConn = null;

        public UdpClient() : base(SocketType.Dgram, ProtocolType.Udp)
        {
        }

        public void Start(SendDataHandle handle)
        {
            thConn = new Thread(() =>
            {
                OnConnect();
                handle();
            });
            thConn.IsBackground = true;
            thConn.Start();
        }

        public virtual void SendData(byte[] data)
        {
            base.SendTo(data, address);
        }

        private void OnConnect()
        {
            if (address == null) { throw new Exception("Set IP first"); }
        }
    }
}
