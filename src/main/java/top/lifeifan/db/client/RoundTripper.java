package top.lifeifan.db.client;

import top.lifeifan.db.transport.Package;
import top.lifeifan.db.transport.Packager;

public class RoundTripper {

    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
