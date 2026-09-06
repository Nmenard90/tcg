/**
 * StorageRoutes — HTTP endpoints for physical storage (boxes, drawers, card assignment).
 *
 * HOW IT WORKS
 *   Pure request/response plumbing over StorageService — parse, delegate,
 *   respond. No business logic here.
 *
 * ENDPOINTS
 *   GET    /api/storage/:userId/boxes                — list boxes with drawers
 *   POST   /api/storage/:userId/boxes                — create a box
 *   PUT    /api/storage/boxes/:boxId                 — rename/reorder a box
 *   DELETE /api/storage/boxes/:boxId                 — delete a box
 *   POST   /api/storage/boxes/:boxId/drawers         — create a drawer
 *   PUT    /api/storage/drawers/:drawerId            — rename/reorder a drawer
 *   DELETE /api/storage/drawers/:drawerId            — delete a drawer
 *   GET    /api/storage/drawers/:drawerId/cards      — cards in a drawer
 *   GET    /api/storage/:userId/unassigned           — owned cards with no drawer
 *   POST   /api/storage/:userId/assign               — assign cards to a drawer
 *   DELETE /api/storage/:userId/assign/:cardId       — unassign a card
 */

package com.poketracker.api

import com.poketracker.models.User
import com.poketracker.service.StorageService
import zio.*
import zio.http.*
import zio.json.*

object StorageRoutes:

  /** kind absent -> "box", same as it always was before display cases existed. */
  private case class CreateBoxRequest(name: String, kind: Option[String], boxType: Option[String], capacity: Option[Int], color: Option[String])
  private given JsonDecoder[CreateBoxRequest] = DeriveJsonDecoder.gen

  private case class CreateDrawerRequest(name: String)
  private given JsonDecoder[CreateDrawerRequest] = DeriveJsonDecoder.gen

  /** name/position are both optional — a PUT only changes whichever fields are present. */
  private case class UpdateRequest(name: Option[String], position: Option[Int])
  private given JsonDecoder[UpdateRequest] = DeriveJsonDecoder.gen

  private case class AssignRequest(cardIds: List[String], drawerId: String)
  private given JsonDecoder[AssignRequest] = DeriveJsonDecoder.gen

  val routes: Routes[StorageService & User, Nothing] = Routes(

    Method.GET / "api" / "storage" / string("userId") / "boxes" -> handler {
      (userId: String, _: Request) =>
        ZIO.serviceWithZIO[StorageService](_.getBoxes(userId))
          .map(boxes => Response.json(boxes.toJson))
          .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.POST / "api" / "storage" / string("userId") / "boxes" -> handler {
      (userId: String, req: Request) =>
        (for
          body   <- req.body.asString
          parsed <- ZIO.fromEither(body.fromJson[CreateBoxRequest])
                      .mapError(e => RuntimeException(s"Bad request: $e"))
          box    <- ZIO.serviceWithZIO[StorageService](_.createBox(userId, parsed.name, parsed.kind.getOrElse("box"), parsed.boxType.getOrElse("custom"), parsed.capacity.getOrElse(0), parsed.color.getOrElse("#B99B67")))
        yield Response.json(box.toJson).status(Status.Created)
        ).catchAll(e => ZIO.succeed(Response.badRequest(e.getMessage)))
    },

    Method.PUT / "api" / "storage" / "boxes" / string("boxId") -> handler {
      (boxId: String, req: Request) =>
        (for
          body   <- req.body.asString
          parsed <- ZIO.fromEither(body.fromJson[UpdateRequest])
                      .mapError(e => RuntimeException(s"Bad request: $e"))
          user   <- ZIO.service[User]
          // ZIO.foreach over an Option: runs only if that field was sent.
          _      <- ZIO.foreach(parsed.name)(n => ZIO.serviceWithZIO[StorageService](_.renameBox(user.id, boxId, n)))
          _      <- ZIO.foreach(parsed.position)(p => ZIO.serviceWithZIO[StorageService](_.reorderBox(user.id, boxId, p)))
        yield Response.json("""{"ok": true}""")
        ).catchAll(e => ZIO.succeed(Response.badRequest(e.getMessage)))
    },

    Method.DELETE / "api" / "storage" / "boxes" / string("boxId") -> handler {
      (boxId: String, _: Request) =>
        (for
          user <- ZIO.service[User]
          _    <- ZIO.serviceWithZIO[StorageService](_.deleteBox(user.id, boxId))
        yield ())
          .map(_ => Response.json("""{"ok": true}"""))
          .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.POST / "api" / "storage" / "boxes" / string("boxId") / "drawers" -> handler {
      (boxId: String, req: Request) =>
        (for
          body   <- req.body.asString
          parsed <- ZIO.fromEither(body.fromJson[CreateDrawerRequest])
                      .mapError(e => RuntimeException(s"Bad request: $e"))
          user   <- ZIO.service[User]
          drawer <- ZIO.serviceWithZIO[StorageService](_.createDrawer(user.id, boxId, parsed.name))
        yield Response.json(drawer.toJson).status(Status.Created)
        ).catchAll(e => ZIO.succeed(Response.badRequest(e.getMessage)))
    },

    Method.PUT / "api" / "storage" / "drawers" / string("drawerId") -> handler {
      (drawerId: String, req: Request) =>
        (for
          body   <- req.body.asString
          parsed <- ZIO.fromEither(body.fromJson[UpdateRequest])
                      .mapError(e => RuntimeException(s"Bad request: $e"))
          user   <- ZIO.service[User]
          _      <- ZIO.foreach(parsed.name)(n => ZIO.serviceWithZIO[StorageService](_.renameDrawer(user.id, drawerId, n)))
          _      <- ZIO.foreach(parsed.position)(p => ZIO.serviceWithZIO[StorageService](_.reorderDrawer(user.id, drawerId, p)))
        yield Response.json("""{"ok": true}""")
        ).catchAll(e => ZIO.succeed(Response.badRequest(e.getMessage)))
    },

    Method.DELETE / "api" / "storage" / "drawers" / string("drawerId") -> handler {
      (drawerId: String, _: Request) =>
        (for
          user <- ZIO.service[User]
          _    <- ZIO.serviceWithZIO[StorageService](_.deleteDrawer(user.id, drawerId))
        yield ())
          .map(_ => Response.json("""{"ok": true}"""))
          .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.GET / "api" / "storage" / "drawers" / string("drawerId") / "cards" -> handler {
      (drawerId: String, _: Request) =>
        (for
          user  <- ZIO.service[User]
          cards <- ZIO.serviceWithZIO[StorageService](_.getDrawerCards(user.id, drawerId))
        yield cards)
          .map(cards => Response.json(cards.toJson))
          .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.GET / "api" / "storage" / string("userId") / "unassigned" -> handler {
      (userId: String, _: Request) =>
        ZIO.serviceWithZIO[StorageService](_.getUnassignedCards(userId))
          .map(cards => Response.json(cards.toJson))
          .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.POST / "api" / "storage" / string("userId") / "assign" -> handler {
      (userId: String, req: Request) =>
        (for
          body   <- req.body.asString
          parsed <- ZIO.fromEither(body.fromJson[AssignRequest])
                      .mapError(e => RuntimeException(s"Bad request: $e"))
          result <- ZIO.serviceWithZIO[StorageService](_.assignCards(userId, parsed.cardIds, parsed.drawerId))
        yield Response.json(result.toJson)
        ).catchAll(e => ZIO.succeed(Response.badRequest(e.getMessage)))
    },

    Method.DELETE / "api" / "storage" / string("userId") / "assign" / string("cardId") -> handler {
      (userId: String, cardId: String, _: Request) =>
        ZIO.serviceWithZIO[StorageService](_.unassignCard(userId, cardId))
          .map(_ => Response.json("""{"ok": true}"""))
          .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    }
  )
