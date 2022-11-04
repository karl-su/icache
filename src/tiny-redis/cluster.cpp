#include "server.h"
#include "endianconv.h"

static void clusterReplyMultiBulkSlots(client *c) 
{
    /* Format: 1) 1) start slot
     *            2) end slot
     *            3) 1) master IP
     *               2) master port
     *               3) node ID
     *            4) 1) replica IP
     *               2) replica port
     *               3) node ID
     *           ... continued until done
     */

    int shards = 0;
    void *slot_replylen = addDeferredMultiBulkLength(c);

    {
        void* nested_replylen = addDeferredMultiBulkLength(c);
        int nested_elements = 3; /* slots(2) + master addr (1). */

        addReplyLongLong(c, 0); /* only one slot; low==high */
        addReplyLongLong(c, 0x4000);

        /* First node reply position is always the master */
        addReplyMultiBulkLen(c, 3);
        addReplyBulkCString(c, "192.168.1.226");
        addReplyLongLong(c, 10000);
        addReplyBulkCBuffer(c, "icache-1", 8);

        setDeferredMultiBulkLength(c, nested_replylen, nested_elements);
        shards++;
    }

    setDeferredMultiBulkLength(c, slot_replylen, shards);
}

void clusterCommand(client* c)
{
    if (!strcasecmp((const char*)c->argv[1]->ptr, "slots") && c->argc == 2)
    {
        clusterReplyMultiBulkSlots(c);      
    }
    else
    {
        addReplyError(c, "Wrong CLUSTER subcommand or number of arguments");
    }
}



