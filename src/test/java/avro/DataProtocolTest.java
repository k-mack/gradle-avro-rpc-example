package avro;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.Callback;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.junit.Test;

import avro.generated.DataProtocol;
import avro.generated.User;

public class DataProtocolTest {

    private static final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress(
            "localhost", 8080);

    private class ProtocolImpl implements DataProtocol {
        private final Map<String, User> backend = new HashMap<String, User>();

        {
            backend.put("JohnDoe123@gmail.com".toLowerCase(Locale.ENGLISH),
                    User.newBuilder().setFirstName("John").setLastName("Doe")
                            .setNickName("JD").setEmail("JohnDoe123@gmail.com")
                            .build());
        }

        @Override
        public User retrieveUser(CharSequence email) throws AvroRemoteException {
            System.out.println("SERVER: looking through backend...");
            return backend.get(email.toString().toLowerCase(Locale.ENGLISH));
        }
    }

    @Test
    public void testNettySuccessResponse() throws IOException {
        NettyServer server = new NettyServer(new SpecificResponder(
                DataProtocol.class, new ProtocolImpl()), SERVER_ADDRESS);
        server.start();
        System.out.println("SERVER: listening on " + SERVER_ADDRESS);

        NettyTransceiver client = new NettyTransceiver(SERVER_ADDRESS);

        DataProtocol.Callback call = SpecificRequestor.getClient(
                DataProtocol.Callback.class, client);

        call.retrieveUser("jOhNdOe123@GmAiL.cOm", new Callback<User>() {
            @Override
            public void handleResult(User result) {
                System.out.println("CLIENT: found " + result);
                assertTrue(result.getFirstName().equals("John"));
                assertTrue(result.getLastName().equals("Doe"));
                assertTrue(result.getNickName().equals("JD"));
            }

            @Override
            public void handleError(Throwable error) {
                System.out.println("CLIENT: error " + error);
                fail();
            }
        });

        simulateClientRunning();

        client.close(true);
        server.close();
    }

    @Test
    public void testNettyErrorResponse() throws IOException {
        final NettyServer server = new NettyServer(new SpecificResponder(
                DataProtocol.class, new ProtocolImpl()), SERVER_ADDRESS);
        server.start();
        System.out.println("SERVER: listening on " + SERVER_ADDRESS);

        NettyTransceiver client = new NettyTransceiver(SERVER_ADDRESS);

        DataProtocol.Callback call = SpecificRequestor.getClient(
                DataProtocol.Callback.class, client);

        call.retrieveUser("jOhNdOe@GmAiL.cOm", new Callback<User>() {
            @Override
            public void handleResult(User result) {
                System.out.println("CLIENT: found " + result);
                fail();
            }

            @Override
            public void handleError(Throwable error) {
                System.out.println("CLIENT: error " + error);
                assertTrue(true);
            }
        });

        simulateClientRunning();

        client.close(true);
        server.close();
    }

    private static void simulateClientRunning() {
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
        System.out.println("CLIENT: .");
    }

}
