using log4net;
using SuperSocket.SocketBase;
using SuperSocket.SocketBase.Command;
using SuperSocket.SocketBase.Config;
using SuperSocket.SocketBase.Protocol;
using SuperSocket.WebSocket;
using SuperSocket.WebSocket.Protocol;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace BLLService
{
    public class SSession : WebSocketSession<SSession>
    {
        public static ILog log = LogManager.GetLogger("WebSocketService");
        protected override void OnSessionStarted()
        {
            base.OnSessionStarted();
            log.Debug(base.RemoteEndPoint);
            log.Debug(base.LocalEndPoint);
        }

        protected override void OnSessionClosed(CloseReason reason)
        {

            base.OnSessionClosed(reason);
        }

        protected override void HandleUnknownRequest(IWebSocketFragment requestInfo)
        {
            base.HandleUnknownRequest(requestInfo);
        }

        protected override void HandleException(Exception e)
        {
            base.HandleException(e);
        }
    }
    public enum SServerCmdType
    {
        SignIn,
        SignOut,
        Message,
        SystemError,
        Unknown
    }
    public class RequestData
    {
        /// <summary>
        /// 
        /// </summary>
        public SServerCmdType cmdType { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string msg { get; set; }
    }

    public class SServer : WebSocketServer<SSession>
    {
        public static ILog log;
        public static Dictionary<string, SSession> ht = new Dictionary<string, SSession>();

        protected override void OnNewSessionConnected(SSession session)
        {
            base.OnNewSessionConnected(session);
        }

        protected override void OnSessionClosed(SSession session , CloseReason reason)
        {
            base.OnSessionClosed(session, reason);
        }

        public RequestData ParseRequestData(string data, out string error)
        {
            error = "解析失败";
            int cmd;
            RequestData result = new RequestData();

            if (data.Length == 0)
            {
                error = "数据验证失败";
                return null;
            }

            if(!int.TryParse(data.Substring(0, 2), out cmd))
            {
                error = "指令解析失败";
                return null;
            }

            switch (cmd)
            {
                case (int)SServerCmdType.SignIn:
                    result.cmdType = SServerCmdType.SignIn;
                    break;
                case (int)SServerCmdType.SignOut:
                    result.cmdType = SServerCmdType.SignOut;
                    break;
                case (int)SServerCmdType.Message:
                    result.cmdType = SServerCmdType.Message;
                    break;
                default:
                    result.cmdType = SServerCmdType.Unknown;
                    break;
            }

            result.msg = data.Substring(1);

            return result;
        }

        protected void OnNewMessageReceived(SSession session, string data)
        {
            string error;

            if(data == "PING")
            {
                ;
                Console.WriteLine("PING,sending PONG,{0}", session.RemoteEndPoint.ToString());
                session.Send("PONG");
                return;
            }

            RequestData result = ParseRequestData(data, out error);
            if(result == null || result.cmdType == SServerCmdType.Unknown)
            {
                session.Send(error);
                return;
            }

            session.Send("success");
        }

        public SServer(string ip, int? port = null)
        {
            ip = string.IsNullOrWhiteSpace(ip) ? "0.0.0.0" : ip;
            port = !port.HasValue ? 7071 : port;

            base.NewMessageReceived += OnNewMessageReceived;
            
            ServerConfig config = new ServerConfig();

            config.Ip = ip;
            config.Port = port.Value;
            config.MaxConnectionNumber = int.MaxValue / 4096;
            config.Name = "SuperSocket";

            if (!base.Setup(config))
            {
                log.ErrorFormat("websocket server setup failed.");
            }
        }
    }
}
