// See https://aka.ms/new-console-template for more information
using EventStore.Client;
using System.Text.Json;
using System.Threading.Channels;



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
List<string> streamNames = ["api/user/addUser", "api/user/deleteUser"];

// Event listeleri
List<SuccessRequestEvent> successRequestEvents = [];
List<UnSuccessRequestEvent> unSuccessRequestEvents = [];

// Channel oluştur
var channel = Channel.CreateUnbounded<object>();

// Her stream için abonelik başlat
foreach (var item in streamNames)
{
    await eventStore.SubscribeToStreamAsync(
        streamName: item,
        start: FromStream.Start,
        eventAppeared: async (streamSubscription, resolvedEvent, cancellationToken) =>
        {
            string eventType = resolvedEvent.Event.EventType;
            var type = Type.GetType(eventType);
            object @event = JsonSerializer.Deserialize(resolvedEvent.Event.Data.ToArray(), type)!;
            await channel.Writer.WriteAsync(@event);
        }
    );
}

var readerTask = Task.Run(async () =>
{
    int successRequestEventCount = 0;
    int unSuccessRequestEventCount = 0;

    await foreach (var e in channel.Reader.ReadAllAsync())
    {
        switch (e)
        {
            case SuccessRequestEvent sr:
                successRequestEvents.Add(sr);
                successRequestEventCount++;
                break;
            case UnSuccessRequestEvent usr:
                unSuccessRequestEvents.Add(usr);
                unSuccessRequestEventCount++;
                break;

        }

        if (successRequestEventCount != 1 && unSuccessRequestEventCount != 1)
        {
            if (successRequestEventCount == successRequestEvents.Count)
                break;

            if (unSuccessRequestEventCount == unSuccessRequestEvents.Count)
                break;
        }


    }
    channel.Writer.Complete();

});

// Event'leri bekle
await readerTask;

// Event'leri sırala ve yazdır
if (successRequestEvents.Any())
{
    var highLastSuccessRequestEvent = successRequestEvents.OrderBy(y => y.RequestTime).Last();
    Console.WriteLine($"Istek suresinde bir artış gözlendi {highLastSuccessRequestEvent.GetType().Name} " + highLastSuccessRequestEvent.RequestTime + " " + highLastSuccessRequestEvent.RequestUrl);
}

if (unSuccessRequestEvents.Any())
{
    var highLastInSuccessRequestEvents = unSuccessRequestEvents.OrderBy(y => y.RequestTime).Last();
    Console.WriteLine($"Istek suresinde bir artış gözlendi {highLastInSuccessRequestEvents.GetType().Name} " + highLastInSuccessRequestEvents.RequestTime + " " + highLastInSuccessRequestEvents.RequestUrl);
}



#endregion


Console.Read();




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