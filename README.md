# maxwell-stream

Provides a framework for zendesk/maxwell with multi-threaded message handling.

The goal would be to provide this as a streaming MapReduce application for feeding downstream data processing needs.


## Custom Handler Functions
Implement your own message handler functions.


Delete

```
import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.cdc.MessageType;
import com.steckytech.maxwell.cdc.Processed;
import com.steckytech.maxwell.consumer.Handler;

public class DeleteRow extends Handler<CDCEvent> {

    public DeleteRow() {
        super(MessageType.delete);
    }

    @Override
    public Processed apply(CDCEvent e) {

        // your code here

        return new Processed(e);
    }

}

```


## stream.properties
Configuration file declares classes to use for message handling.


