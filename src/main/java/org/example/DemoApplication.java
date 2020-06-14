package org.example;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.concurrent.ExecutionException;

public class DemoApplication {
    private static final Location LOCATION = new Location(new Namespace("s21634"), "data");

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        RiakNode node = new RiakNode.Builder().withRemoteAddress("127.0.0.1").withRemotePort(8087).build();
        RiakCluster cluster = new RiakCluster.Builder(node).build();
        cluster.start();
        //SAVE
        System.out.println("Saving...");
        RiakClient client = new RiakClient(cluster);
        RiakObject object = new RiakObject().setContentType("text/plain").setValue(BinaryValue.create("Some data"));
        StoreValue storeOp = new StoreValue.Builder(object).withLocation(LOCATION).build();
        client.execute(storeOp);
        //READ
        System.out.println("Reading...");
        FetchValue fetchOp = new FetchValue.Builder(LOCATION).build();
        RiakObject fetchedObject = client.execute(fetchOp).getValue(RiakObject.class);
        System.out.println("Object now is:");
        System.out.println(fetchedObject.getValue());
        //UPDATE
        System.out.println("Updating...");
        fetchedObject.setValue(BinaryValue.create("Some other data"));
        StoreValue updateOp = new StoreValue.Builder(fetchedObject).withLocation(LOCATION).build();
        client.execute(updateOp);
        readObject(client);
        //DELETE
        System.out.println("deleting...");
        DeleteValue deleteOp = new DeleteValue.Builder(LOCATION).build();
        client.execute(deleteOp);
        readObject(client);
    }

    private static void readObject(RiakClient client) throws ExecutionException, InterruptedException {
        FetchValue fetchOp = new FetchValue.Builder(LOCATION).build();
        RiakObject fetchedObject = client.execute(fetchOp).getValue(RiakObject.class);
        if (fetchedObject != null) {
            System.out.println("Object now is:");
            System.out.println(fetchedObject.getValue());
        } else {
            System.out.println("Object doesn't exist");
        }
    }

}
