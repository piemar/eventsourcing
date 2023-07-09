public async Task<WarehouseProduct> Get(string sku)
{
    var streamName = GetStreamName(sku);

    var snapshot = await GetSnapshot(sku);
    var warehouseProduct = new WarehouseProduct(sku, snapshot.State);

    StreamEventsSlice currentSlice;
    var nextSliceStart = snapshot.Version + 1;
    do
    {
        currentSlice = await _connection.ReadStreamEventsForwardAsync(
            streamName,
            nextSliceStart,
            200,
            false
        );

        nextSliceStart = currentSlice.NextEventNumber;

        foreach (var evnt in currentSlice.Events)
        {
            var eventObj = DeserializeEvent(evnt);
            warehouseProduct.ApplyEvent(eventObj);
        }
    } while (!currentSlice.IsEndOfStream);

    return warehouseProduct;
}

private async Task<Snapshot> GetSnapshot(string sku)
{
    var streamName = GetSnapshotStreamName(sku);
    var slice = await _connection.ReadStreamEventsBackwardAsync(streamName, (long)StreamPosition.End, 1, false);
    if (slice.Events.Any())
    {
        var evnt = slice.Events.First();
        var json = Encoding.UTF8.GetString(evnt.Event.Data);
        return JsonConvert.DeserializeObject<Snapshot>(json);
    }

    return new Snapshot();
}