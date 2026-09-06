/**
 * CardRoutes — HTTP endpoints for cards and sets.
 *
 * HOW IT WORKS
 *   Pure request/response plumbing over CardService — the only place in
 *   the app that knows about HTTP; services/repositories don't.
 *
 * ENDPOINTS
 *   GET /api/sets                          — all sets, newest first
 *   GET /api/cards/:setId                  — all cards in a set with prices
 *   GET /api/cards/id/:cardId              — single card by ID
 *   GET /api/cards/id/:cardId/price-history
 *   GET /api/search?q=name                 — search cards by name
 *
 * Catalog refresh operations deliberately are not exposed over HTTP. The
 * application has no trustworthy admin authorization mechanism yet, so
 * mounting them for ordinary authenticated users would not be safe.
 */

package com.poketracker.api

import com.poketracker.models.Card
import com.poketracker.service.CardService
import zio.*
import zio.http.*
import zio.json.*

object CardRoutes:

  private case class BrowseResponse(cards: List[Card], total: Int)
  private given JsonEncoder[BrowseResponse] = DeriveJsonEncoder.gen

  private val allowedSorts = Set("name", "price", "number", "artist", "set")

  val publicRoutes: Routes[CardService, Nothing] = Routes(

    Method.GET / "api" / "sets" -> handler {
      ZIO.serviceWithZIO[CardService](_.getSets)
        .map(sets => Response.json(sets.toJson))
        .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    // Registered ahead of "/api/cards/:setId" — that route's string()
    // matcher would otherwise swallow "browse" as a literal set ID.
    Method.GET / "api" / "cards" / "browse" -> handler { (req: Request) =>
      val qp = req.url.queryParams
      val sort = qp.getAll("sort").headOption.filter(allowedSorts).getOrElse("name")
      val dir = if qp.getAll("dir").headOption.contains("desc") then "desc" else "asc"
      val page = qp.getAll("page").headOption.flatMap(_.toIntOption).map(_.max(0)).getOrElse(0)
      val pageSize = qp.getAll("pageSize").headOption.flatMap(_.toIntOption).map(n => n.max(1).min(100)).getOrElse(60)

      ZIO.serviceWithZIO[CardService](_.browseCards(sort, dir, page, pageSize))
        .map { case (cards, total) => Response.json(BrowseResponse(cards, total).toJson) }
        .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.GET / "api" / "cards" / string("setId") -> handler { (setId: String, _: Request) =>
      ZIO.serviceWithZIO[CardService](_.getCardsBySet(setId))
        .map(cards => Response.json(cards.toJson))
        .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.GET / "api" / "cards" / "id" / string("cardId") -> handler { (cardId: String, _: Request) =>
      ZIO.serviceWithZIO[CardService](_.getCardById(cardId))
        .map {
          case Some(card) => Response.json(card.toJson)
          case None       => Response.notFound
        }
        .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.GET / "api" / "cards" / "id" / string("cardId") / "price-history" -> handler { (cardId: String, _: Request) =>
      ZIO.serviceWithZIO[CardService](_.getPriceHistory(cardId))
        .map(points => Response.json(points.toJson))
        .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    },

    Method.GET / "api" / "search" -> handler { (req: Request) =>
      val query = req.url.queryParams.getAll("q").headOption.getOrElse("")
      if query.length < 2 then
        ZIO.succeed(Response.badRequest("Query must be at least 2 characters"))
      else
        ZIO.serviceWithZIO[CardService](_.searchCards(query))
          .map(cards => Response.json(cards.toJson))
          .catchAll(e => ZIO.succeed(Response.internalServerError(e.getMessage)))
    }
  )
