/**
 * RipRoutes — HTTP endpoints for the Rip Tracker feature (box-opening
 * sessions and the rip-or-hold verdict). All logic lives in RipTrackerService.
 *
 * Session routes require the authenticated User supplied by AuthGuard. The
 * read-only verdict remains public; no frontend/UI work is enabled here.
 *
 * ENDPOINTS
 *   POST /api/rip-sessions                    — start a session for a product
 *   POST /api/rip-sessions/:id/pulls           — record a scanned card into a pack
 *   GET  /api/rip-sessions/:id                 — session recap (packs + pulls)
 *   GET  /api/rip-tracker/verdict/:productId   — rip-or-hold verdict
 */

package com.poketracker.api

import com.poketracker.models.*
import com.poketracker.service.{
  RipTrackerService, PullResult, SessionRecap, Verdict,
  GuaranteedFloorResult, PerPackEvResult, DistributionStats, CeilingCheck, MarginalValueResult
}
import zio.*
import zio.http.*
import zio.json.*

object RipRoutes:

  private case class CreateSessionRequest(productId: String)
  private given JsonDecoder[CreateSessionRequest] = DeriveJsonDecoder.gen

  private case class RecordPullRequest(ripPackId: String, cardId: String, condition: String)
  private given JsonDecoder[RecordPullRequest] = DeriveJsonDecoder.gen

  // None of these model types carry their own JsonEncoder, so this file
  // derives each right where it's needed to build a response.
  private given JsonEncoder[RipSession] = DeriveJsonEncoder.gen
  private given JsonEncoder[RipPack]    = DeriveJsonEncoder.gen
  private given JsonEncoder[Pull]       = DeriveJsonEncoder.gen
  private given JsonEncoder[PullResult] = DeriveJsonEncoder.gen
  private given JsonEncoder[SessionRecap] = DeriveJsonEncoder.gen
  private given JsonEncoder[GuaranteedFloorResult]  = DeriveJsonEncoder.gen
  private given JsonEncoder[PerPackEvResult]        = DeriveJsonEncoder.gen
  private given JsonEncoder[DistributionStats]      = DeriveJsonEncoder.gen
  private given JsonEncoder[CeilingCheck]           = DeriveJsonEncoder.gen
  private given JsonEncoder[MarginalValueResult]    = DeriveJsonEncoder.gen

  // Verdict is composed of the types above, so its encoder must come after them.
  private given JsonEncoder[Verdict]                = DeriveJsonEncoder.gen

  private case class CreateSessionResponse(session: RipSession, packs: List[RipPack])
  private given JsonEncoder[CreateSessionResponse] = DeriveJsonEncoder.gen

  val sessionRoutes: Routes[RipTrackerService & User, Nothing] = Routes(

    Method.POST / "api" / "rip-sessions" -> handler { (req: Request) =>
      (for
        body   <- req.body.asString
        parsed <- ZIO.fromEither(body.fromJson[CreateSessionRequest])
                    .mapError(e => RuntimeException(s"Bad request: $e"))
        user             <- ZIO.service[User]
        (session, packs) <- ZIO.serviceWithZIO[RipTrackerService](
                               _.createSession(user.id, parsed.productId)
                             )
      yield Response.json(CreateSessionResponse(session, packs).toJson)
      ).catchAll(_ => ZIO.succeed(Response.badRequest("Could not create rip session")))
    },

    Method.POST / "api" / "rip-sessions" / string("id") / "pulls" -> handler {
      (sessionId: String, req: Request) =>
        (for
          body   <- req.body.asString
          parsed <- ZIO.fromEither(body.fromJson[RecordPullRequest])
                      .mapError(e => RuntimeException(s"Bad request: $e"))
          user   <- ZIO.service[User]
          result <- ZIO.serviceWithZIO[RipTrackerService](
                      _.recordPull(user.id, sessionId, parsed.ripPackId, parsed.cardId, parsed.condition)
                    )
        yield Response.json(result.toJson)
        ).catchAll(_ => ZIO.succeed(Response.badRequest("Could not record pull")))
    },

    Method.GET / "api" / "rip-sessions" / string("id") -> handler { (id: String, _: Request) =>
      (for
        user  <- ZIO.service[User]
        recap <- ZIO.serviceWithZIO[RipTrackerService](_.getRecap(user.id, id))
      yield recap)
        .map {
          case Some(recap) => Response.json(recap.toJson)
          case None        => Response.status(Status.NotFound)
        }
        .catchAll(_ => ZIO.succeed(Response.internalServerError("Could not load rip session")))
    }
  )

  val verdictRoutes: Routes[RipTrackerService, Nothing] = Routes(
    Method.GET / "api" / "rip-tracker" / "verdict" / string("productId") -> handler {
      (productId: String, req: Request) =>
        val q = req.url.queryParams
        // A non-numeric value is silently treated as absent, not an error.
        val chaseThreshold       = q.getAll("chaseThreshold").headOption.flatMap(_.toDoubleOption)
        val projectedSealedPrice = q.getAll("projectedSealedPrice").headOption.flatMap(_.toDoubleOption)

        ZIO.serviceWithZIO[RipTrackerService](
          // Public verdicts are catalog-only. A query-string identity must not
          // expose collection-derived marginal values.
          _.getVerdict(productId, None, chaseThreshold, projectedSealedPrice)
        )
          .map {
            case Some(verdict) => Response.json(verdict.toJson)
            case None          => Response.status(Status.NotFound)
          }
          .catchAll(_ => ZIO.succeed(Response.internalServerError("Could not calculate verdict")))
    }
  )
