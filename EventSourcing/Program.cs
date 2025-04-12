// See https://aka.ms/new-console-template for more information
using EventStore.Client;
using System.Collections.Concurrent;
using System.Text.Json;


ConcurrentDictionary<string, TimeSpan> lastPrintedSuccessEventTimes = new ConcurrentDictionary<string, TimeSpan>();
ConcurrentDictionary<string, TimeSpan> lastPrintedUnsuccessEventTimes = new ConcurrentDictionary<string, TimeSpan>();

Lock _lock = new Lock();

var eventStoreClientSettings = EventStoreClientSettings.Create("esdb://admin:changeit@localhost:2113?tls=false&tlsVerifyCert=false");
EventStoreClient eventStore = new EventStoreClient(eventStoreClientSettings);


#region CreateEvent
//int i = 0;
//do
//{
//    var createOrder = new CreateOrderEvent()
//    {
//        BuyerId = 1 + i,
//        OrderId = 2 + i,
//        OrderItems = new List<OrderItems>()
//    {
//        new OrderItems{ Count=1,ItemId=1,Name="Ayakkabı",Price=15},
//        new OrderItems{ Count=4,ItemId=2,Name="Ayakkabı",Price=20},
//        new OrderItems{ Count=2,ItemId=3,Name="Ayakkabı",Price=10},
//    }
//    };

//    EventData @event = new EventData(
//        eventId: Uuid.NewUuid(),
//        type: createOrder.GetType().Name,
//        data: JsonSerializer.SerializeToUtf8Bytes(createOrder));

//    await eventStore.AppendToStreamAsync(
//       streamName: "order-stream",
//       expectedState: StreamState.Any,
//       eventData: new[] { @event }
//       );

//    i++;
//}
//while (i <= 10);
#endregion


#region Subscribe
//await eventStore.SubscribeToStreamAsync(
//    streamName: "order-stream",
//    start: FromStream.Start,
//    eventAppeared: async (streamSubscription, resolvedEvent, cancellationToken) =>
//    {
//        string eventType = resolvedEvent.Event.EventType;
//        Console.WriteLine(eventType);
//        var data = JsonSerializer.Deserialize(resolvedEvent.Event.Data.ToArray(),
//            Type.GetType(eventType)
//            );
//        Console.Write(eventType + " " + JsonSerializer.Serialize(data));


//    });


#endregion



#region Checkpoint Example

//SuccessRequestEvent successRequestEvent = new()
//{
//    RequestTime = DateTime.UtcNow - DateTime.UtcNow.AddMinutes(-2),
//    RequestUrl = "api/user/addUser",
//    StatusCode = 200
//};

//SuccessRequestEvent successRequestEvent1 = new()
//{
//    RequestTime = DateTime.UtcNow - DateTime.UtcNow.AddMinutes(-1),
//    RequestUrl = "api/user/addUser",
//    StatusCode = 200
//};

//UnSuccessRequestEvent unSuccessRequestEvent = new()
//{
//    RequestTime = DateTime.UtcNow - DateTime.UtcNow.AddMinutes(-1),
//    RequestUrl = "api/user/deleteUser",
//    StatusCode = 400
//};

//UnSuccessRequestEvent unSuccessRequestEvent1 = new()
//{
//    RequestTime = DateTime.UtcNow - DateTime.UtcNow.AddMinutes(-5),
//    RequestUrl = "api/user/deleteUser",
//    StatusCode = 400
//};

//EventData eventData = new(
//    eventId: Uuid.NewUuid(),
//    type: successRequestEvent.GetType().Name,
//    data: JsonSerializer.SerializeToUtf8Bytes(successRequestEvent));

//EventData eventData2 = new(
//    eventId: Uuid.NewUuid(),
//    type: successRequestEvent1.GetType().Name,
//    data: JsonSerializer.SerializeToUtf8Bytes(successRequestEvent1));

//EventData eventData3 = new(
//    eventId: Uuid.NewUuid(),
//    type: unSuccessRequestEvent.GetType().Name,
//    data: JsonSerializer.SerializeToUtf8Bytes(unSuccessRequestEvent));

//EventData eventData4 = new(
//    eventId: Uuid.NewUuid(),
//    type: unSuccessRequestEvent1.GetType().Name,
//    data: JsonSerializer.SerializeToUtf8Bytes(unSuccessRequestEvent1));



//await eventStore.AppendToStreamAsync(
//    streamName: "api/user/addUser",
//    expectedState: StreamState.Any,
//    eventData: [eventData]
//    );



//await eventStore.AppendToStreamAsync(
//    streamName: "api/user/addUser",
//    expectedState: StreamState.Any,
//    eventData: [eventData2]
//    );



//await eventStore.AppendToStreamAsync(
//    streamName: "api/user/deleteUser",
//    expectedState: StreamState.Any,
//    eventData: [eventData3]
//    );



//await eventStore.AppendToStreamAsync(
//    streamName: "api/user/deleteUser",
//    expectedState: StreamState.Any,
//    eventData: [eventData4]
//    );

List<SuccessRequestEvent> successRequests = [];
List<UnSuccessRequestEvent> unSuccessRequestEvents = [];
List<string> streamNames = ["api/user/addUser", "api/user/deleteUser"];


//foreach (var streamName in streamNames)
//{
//    await eventStore.SubscribeToStreamAsync(streamName: streamName,
//        start: FromStream.Start,
//        eventAppeared: async (streamSubscription, resolvedEvent, cancellationToken) =>
//        {
//            string eventType = resolvedEvent.Event.EventType;
//            var type = Type.GetType(eventType)!;
//            object @event = JsonSerializer.Deserialize(resolvedEvent.Event.Data.ToArray(), type)!;

//            switch (@event)
//            {
//                case SuccessRequestEvent successRequestEvent:
//                    lastPrintedSuccessEventTimes.AddOrUpdate(
//                        successRequestEvent.RequestUrl,
//                        successRequestEvent.RequestTime,
//                        (key, oldValue) =>
//                        {
//                            if (successRequestEvent.RequestTime > oldValue)
//                            {
//                                Console.WriteLine($"Istek suresinde bir artış gözlendi {successRequestEvent.GetType().Name} " +
//                                                  successRequestEvent.RequestTime + " " + successRequestEvent.RequestUrl);
//                                return successRequestEvent.RequestTime;
//                            }
//                            return oldValue;
//                        });
//                    break;

//                case UnSuccessRequestEvent unSuccessRequestEvent:
//                    lastPrintedUnsuccessEventTimes.AddOrUpdate(
//                        unSuccessRequestEvent.RequestUrl,
//                        unSuccessRequestEvent.RequestTime,
//                        (key, oldValue) =>
//                        {
//                            if (unSuccessRequestEvent.RequestTime > oldValue)
//                            {
//                                Console.WriteLine($"Istek suresinde bir artış gözlendi {unSuccessRequestEvent.GetType().Name} " +
//                                                  unSuccessRequestEvent.RequestTime + " " + unSuccessRequestEvent.RequestUrl);
//                                return unSuccessRequestEvent.RequestTime;
//                            }
//                            return oldValue;
//                        });
//                    break;
//            }
//        });
//}



foreach (var streamName in streamNames)
{
    await eventStore.SubscribeToStreamAsync(streamName: streamName,
    start: FromStream.Start,
    eventAppeared: async (streamSubscription, resolvedEvent, cancellationToken) =>
    {
        string eventType = resolvedEvent.Event.EventType;
        var type = Type.GetType(eventType)!;
        object @event = JsonSerializer.Deserialize(resolvedEvent.Event.Data.ToArray(), type)!;


        _lock.Enter();

        try
        {
            switch (@event)
            {
                case SuccessRequestEvent successRequestEvent:
                    successRequests.Add(successRequestEvent);
                    if (successRequests.Count > 1)
                    {
                        var lastSuccessRequest = successRequests.OrderBy(y => y.RequestTime).Last();

                        Console.WriteLine("Zaman aşımına uğrayan istek: " + lastSuccessRequest.RequestUrl + " " + lastSuccessRequest.RequestTime);
                    }
                    break;
                case UnSuccessRequestEvent unSuccessRequestEvent:
                    unSuccessRequestEvents.Add(unSuccessRequestEvent);
                    if (unSuccessRequestEvents.Count > 1)
                    {
                        var lastUnSuccessRequest = unSuccessRequestEvents.OrderBy(y => y.RequestTime).Last();

                        Console.WriteLine("Zaman aşımına uğrayan istek: " + lastUnSuccessRequest.RequestUrl + " " + lastUnSuccessRequest.RequestTime);
                    }
                    break;
            }

        }
        finally
        {
            _lock.Exit();
        }



    });
}



Console.Read();

#endregion





public class CreateOrderEvent
{
    public int BuyerId { get; set; }
    public int OrderId { get; set; }
    public decimal TotalPrice { get; set; }
    public List<OrderItems> OrderItems { get; set; }
}
public class OrderItems
{
    public int ItemId { get; set; }
    public string Name { get; set; }
    public decimal Price { get; set; }
    public int Count { get; set; }
}



public class SuccessRequestEvent
{
    public string RequestUrl { get; set; }
    public TimeSpan RequestTime { get; set; }
    public int StatusCode { get; set; }
}
public class UnSuccessRequestEvent
{
    public string RequestUrl { get; set; }
    public TimeSpan RequestTime { get; set; }
    public int StatusCode { get; set; }
}