import data.Product;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.ext.web.Router;
import io.vertx.mutiny.ext.web.RoutingContext;
import io.vertx.mutiny.ext.web.handler.BodyHandler;
import org.hibernate.reactive.mutiny.Mutiny;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Persistence;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class ApiVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(ApiVerticle.class);

  private Mutiny.SessionFactory emf = Persistence.createEntityManagerFactory("pg-demo").unwrap(Mutiny.SessionFactory.class);

  public ApiVerticle() {
  }

  @Override
  public Uni<Void> asyncStart() {
    Router router = Router.router(vertx);

    BodyHandler bodyHandler = BodyHandler.create();
    router.post().handler(bodyHandler::handle);

    router.get("/products").respond(this::listProducts);
    router.get("/products/:id")
      .handler(this::validateProductId)
      .respond(this::fetchProduct);
    router.post("/products")
      .handler(this::validateProduct)
      .handler(this::appendProduct);

    return vertx.createHttpServer()
      .requestHandler(router::handle)
      .listen(8080)
      .onItem().invoke(() -> logger.info("HTTP server listening on port 8080"))
      .replaceWithVoid();
  }

  private Uni<List<Product>> listProducts(RoutingContext rc) {
    return emf.withSession(session -> session.createQuery("from Product", Product.class)
      .getResultList());
  }

  private void validateProductId(RoutingContext rc) {
    try {
      rc.put("productId", Long.parseLong(rc.pathParam("id")));
      rc.next();
    } catch (NumberFormatException e) {
      rc.fail(e);
    }
  }

  private Uni<Product> fetchProduct(RoutingContext rc) {
    long productId = rc.get("productId");
    return emf
      .withSession(session -> session.find(Product.class, productId));
  }

  private void validateProduct(RoutingContext rc) {
    JsonObject json = rc.getBodyAsJson();
    String name;
    BigDecimal price;

    try {
      requireNonNull(json, "The incoming JSON document cannot be null");
      name = requireNonNull(json.getString("name"), "The product name cannot be null");
      price = new BigDecimal(json.getString("price"));

      Product product = new Product();
      product.setName(name);
      product.setPrice(price);

      rc.put("product", product);
      rc.next();
    } catch (Throwable err) {
      logger.error("Could not extract values", err);
      rc.fail(400, err);
    }
  }

  // TODO : cannot handle 201 code currently
  // this is a 4.1 feature
  private void appendProduct(RoutingContext rc) {

    Product product = rc.get("product");
    dispatch("appendProduct", rc, session ->
      session
        .persist(product)
        .chain(session::flush)
        .chain(done -> rc.response().setStatusCode(201).end()));
  }

  private <T> void dispatch(String operation, RoutingContext rc, Function<Mutiny.Session, Uni<T>> block) {
    emf.withSession(block).subscribe().with(
      ok -> logger.info("Served {} request from {}", operation, rc.request().remoteAddress()),
      err -> {
        logger.error("Failed to serve {} request from {}", operation, rc.request().remoteAddress(), err);
        rc.fail(500);
      }
    );
  }
}

