using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;
using ProtoBuf;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {
            Task.Factory.StartNew(Client, TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(Server, TaskCreationOptions.LongRunning);
            Console.ReadKey();


        }
        private static void Client()
        {
            using (var sender = new DealerSocket())
            {
                sender.Options.Identity = Encoding.Unicode.GetBytes("salda".ToString());
                sender.Connect("tcp://127.0.0.1:9045");
                while (true)
                {

                    var message = new NetMQMessage();
                    message.Append("hello");
                    sender.SendMultipartMessage(message);
                    Console.WriteLine("Sent request");
                    //var response = sender.ReceiveMultipartMessage();
                    //Console.WriteLine("Received response");
                    //var itr = response.GetEnumerator();
                    //Console.WriteLine($"{(itr.MoveNext() ? "found" : "didn't find")} greeting frame");
                    //Console.WriteLine($"Greeting is: {itr.Current.ConvertToString()}");
                    
                    var receiveFrameBytes = sender.ReceiveMultipartMessage();
                    using (var ms = new MemoryStream())
                    {
                        var equity = ProtoBufDeserialize<Equity>(receiveFrameBytes[0].Buffer, ms);
                        Console.WriteLine($"Equity: {equity.Value}");
                    }


                    Thread.Sleep(TimeSpan.FromSeconds(5));
                }
            }
        }
        private static void Server()
        {
            using (var receiver = new RouterSocket())
            {
                receiver.Bind("tcp://*:9045");
                while (true)
                {


                    var message = receiver.ReceiveMultipartMessage();
                    Console.WriteLine("Received request for equity");
                    //var itr = message.GetEnumerator();
                    //Console.WriteLine($"{(itr.MoveNext() ? "found" : "didn't find")} identity frame");
                    //Console.Write($"from {itr.Current.ConvertToString(Encoding.Unicode)}");

                    var response = new NetMQMessage();
                    response.Append(message.First);
                    //response.Append("goodbye");
                    //receiver.SendMultipartMessage(response);
                    //Console.WriteLine("Sent response.");
                    using (var ms = new MemoryStream())
                    {
                        var equity = ProtoBufSerialize(new Equity() { Account = new Account(), AccountID = 1, ID = 1, UpdateTime = DateTime.Now, Value = 500000 }, ms);
                      
                        response.Append(equity);
                        response.AppendEmptyFrame();
                        receiver.SendMultipartMessage(response);
                        
                        
                    }

                }
            }
        }

        /// <summary>
        /// Serialize object using protocol buffers.
        /// </summary>
        public static byte[] ProtoBufSerialize(object input, MemoryStream ms)
        {

            try
            {
                ms.SetLength(0);
                Serializer.Serialize(ms, input);
                ms.Position = 0;
                byte[] buffer = new byte[ms.Length];
                ms.Read(buffer, 0, (int) ms.Length);
                return buffer;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            
           

        }

        /// <summary>
        /// Deserialize object of type T using protocol buffers.
        /// </summary>
        public static T ProtoBufDeserialize<T>(byte[] input, MemoryStream ms)
        {
            try
            {

            
            ms.SetLength(0);
            ms.Write(input, 0, input.Length);
            ms.Position = 0;
            return Serializer.Deserialize<T>(ms);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
    [ProtoContract]
    internal class Equity
    {
       

       
        [ProtoMember(1)]
        public int ID { get; set; }
        [ProtoMember(2)]
        public virtual Account Account { get; set; }
        [ProtoMember(3)]
        public int AccountID { get; set; }
        [ProtoMember(4)]
      
        public decimal Value { get; set; }
        [ProtoMember(5)]
        public DateTime UpdateTime { get; set; }
    }
    [ProtoContract]
    internal class Account
    {
    }
}
