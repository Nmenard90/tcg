/**
 * BinderRoutes — HTTP endpoints for binder management.
 *
 * HOW IT WORKS
 *   Pure request/response plumbing over BinderService — parse, delegate,
 *   respond.
 *
 * ENDPOINTS
 *   GET    /api/binders/:userId                            — list binders
 *   POST   /api/binders/:userId                            — create binder
 *   GET    /api/binders/:userId/:binderId                  — get binder with slots
 *   PUT    /api/binders/:userId/:binderId                  — update name/cover/size
 *   DELETE /api/binders/:userId/:binderId                  — delete binder
 *   PUT    /api/binders/:userId/:binderId/slot/:slotIndex  — place/remove card
 */

package com.poketracker.api

import com.poketracker.models.*
import com.poketracker.service.BinderService
import zio.*
import zio.http.*
import zio.json.*

object BinderRoutes:

  private case class CreateBinderRequest(name: String, pocketSize: String)
  private given JsonDecoder[CreateBinderRequest] = DeriveJsonDecoder.gen

  /** Every field optional — a PUT only changes whichever fields are present. */
  private case class UpdateBinderRequest(
    name:       Option[String],
    coverImage: Option[String],
    pocketSize: Option[String]
  )
  private given JsonDecoder[UpdateBinderRequest] = DeriveJsonDecoder.gen

  /** cardId absent (and everything else blank) clears the slot. */
  private case class SlotRequest(
    cardId:   Option[String],
    cardName: Option[String],
    imageUrl: Option[String]
  )
  private given JsonDecoder[SlotRequest] = DeriveJsonDecoder.gen

  val routes: Routes[BinderService, Nothing] = Routes(

    Method.GET / "api" / "binders" / string("userId") -> handler { (userId: String, _: Request) =>
      ZIO.serviceWithZIO[BinderService](_.getBinders(userId))
        .map(binders => Response.json(binders.toJson))
        .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.POST / "api" / "binders" / string("userId") -> handler { (userId: String, req: Request) =>
      (for
        body   <- req.body.asString
        parsed <- ZIO.fromEither(body.fromJson[CreateBinderRequest])
                    .mapError(e => RuntimeException(s"Bad request: $e"))
        size   <- ZIO.attempt(PocketSize.valueOf(parsed.pocketSize))
                    .mapError(_ => RuntimeException(
                      s"Invalid pocketSize '${parsed.pocketSize}'. Must be Four, Nine, or Twelve."
                    ))
        binder <- ZIO.serviceWithZIO[BinderService](_.createBinder(userId, parsed.name, size))
      yield Response.json(binder.toJson).status(Status.Created)
      ).catchAll(e => ZIO.succeed(Response.badRequest(e.getMessage)))
    },

    Method.GET / "api" / "binders" / string("userId") / string("binderId") -> handler {
      (userId: String, binderId: String, _: Request) =>
        ZIO.serviceWithZIO[BinderService](_.getBinder(userId, binderId))
          .map {
            case Some(b) => Response.json(b.toJson)
            case None    => Response.notFound
          }
          .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.PUT / "api" / "binders" / string("userId") / string("binderId") -> handler {
      (userId: String, binderId: String, req: Request) =>
        (for
          body   <- req.body.asString
          parsed <- ZIO.fromEither(body.fromJson[UpdateBinderRequest])
                      .mapError(e => RuntimeException(s"Bad request: $e"))
          _      <- ZIO.foreach(parsed.name)(n =>
                      ZIO.serviceWithZIO[BinderService](_.renameBinder(userId, binderId, n))
                    )
          _      <- ZIO.foreach(parsed.coverImage)(url =>
                      ZIO.serviceWithZIO[BinderService](_.setCover(userId, binderId, url))
                    )
          _      <- ZIO.foreach(parsed.pocketSize)(s =>
                      ZIO.attempt(PocketSize.valueOf(s))
                        .mapError(_ => RuntimeException(
                          s"Invalid pocket size '$s'. Must be Four, Nine, or Twelve."
                        ))
                        .flatMap(size =>
                          ZIO.serviceWithZIO[BinderService](_.resizeBinder(userId, binderId, size))
                        )
                    )
        yield Response.json("""{"ok": true}""")
        ).catchAll(e => ZIO.succeed(Response.badRequest(e.getMessage)))
    },

    Method.DELETE / "api" / "binders" / string("userId") / string("binderId") -> handler {
      (userId: String, binderId: String, _: Request) =>
        ZIO.serviceWithZIO[BinderService](_.deleteBinder(userId, binderId))
          .map(_ => Response.json("""{"ok": true}"""))
          .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.PUT / "api" / "binders" / string("userId") / string("binderId") / "slot" / int("slotIndex") -> handler {
      (userId: String, binderId: String, slotIndex: Int, req: Request) =>
        (for
          body   <- req.body.asString
          parsed <- ZIO.fromEither(body.fromJson[SlotRequest])
                      .mapError(e => RuntimeException(s"Bad request: $e"))
          _      <- parsed.cardId match
                      case None =>
                        ZIO.serviceWithZIO[BinderService](_.removeCard(userId, binderId, slotIndex))
                      case Some(cardId) =>
                        // The client only sends id/name/image — BinderService.placeCard
                        // only persists those three fields, so the rest is left blank
                        // rather than round-tripping a full Card fetch here.
                        val card = Card(cardId, "", parsed.cardName.getOrElse(""),
                                        "", None, None,
                                        CardImage(parsed.imageUrl.getOrElse(""), ""), None, None)
                        ZIO.serviceWithZIO[BinderService](_.placeCard(userId, binderId, slotIndex, card))
        yield Response.json("""{"ok": true}""")
        ).catchAll(e => ZIO.succeed(Response.badRequest(e.getMessage)))
    }
  )
