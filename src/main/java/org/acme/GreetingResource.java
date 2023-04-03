package org.acme;

import io.quarkus.reactive.datasource.runtime.DataSourcesReactiveRuntimeConfig;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Promise;
import io.vertx.core.impl.ContextInternal;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.pgclient.PgConnection;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.impl.PgConnectionImpl;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/test")
public class GreetingResource {
    @Inject
    Vertx vertx;

    @Inject
    DataSourcesReactiveRuntimeConfig reactiveRuntimeConfig;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/leak")
    public Uni<Long> leak() {
        return getConnection().flatMap(c -> c.preparedQuery("SELECT txid_current() as tx").execute().map(RowSet::iterator).map(r -> {
            if (r.hasNext()) {
                return r.next().getLong("tx");
            } else {
                return -1L;
            }
        }).eventually(c::close));
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/noLeak")
    public Uni<Long> noLeak() {
        return getConnection().flatMap(c -> c.preparedQuery("SELECT txid_current() as tx").execute().map(RowSet::iterator).map(r -> {
            if (r.hasNext()) {
                return r.next().getLong("tx");
            } else {
                return -1L;
            }
        }).eventually(() -> cleanUp(c)));
    }


    public Uni<PgConnection> getConnection() {
        var username = "quarkus";
        var password = "quarkus";
        var url = reactiveRuntimeConfig.defaultDataSource.url.get();
        //vertx-reactive:postgresql://localhost:49179/default?loggerLevel=OFF
        //remove every thing before //localhost: or //172.0.0.1:
        var port = Integer.parseInt(url.split("//[\\w.]*:")[1].split("/")[0]);
        var host = url.split("postgresql://")[1].split(":")[0];
        var databaseName = url.split("\\d/")[1].split("\\?")[0];
        PgConnectOptions connectOptions = new PgConnectOptions().setPort(port).setHost(host).setDatabase(databaseName).setUser(username).setPassword(password);
        return PgConnection.connect(vertx, connectOptions);
    }

    public Uni<Void> cleanUp(PgConnection c) {
        ContextInternal context = ContextInternal.current();
        if (context != null) {
            context.removeCloseHook(((PgConnectionImpl) c.getDelegate()).factory());
            Promise<Void> promise = Promise.promise();
            ((PgConnectionImpl) c.getDelegate()).factory().close(promise);
        }
        return c.close();
    }
}