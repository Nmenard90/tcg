/**
 * RipTrackerService — running a rip (box-opening) session and computing
 * the rip-or-hold verdict.
 *
 * HOW IT WORKS
 *   Orchestration layer: ties together session/pull storage
 *   (RipTrackerRepository), real card prices (CardRepository), merging
 *   pulls into the user's collection (CollectionService), and
 *   RipOrHoldEngine's math. This file only feeds RipOrHoldEngine real
 *   data — it never recalculates the math itself.
 *
 * WIRED IN: RipRoutes.sessionRoutes (mutating) requires the authenticated
 * User from AuthGuard; RipRoutes.verdictRoutes (read-only) stays public.
 * UNVERIFIED: whether Migration 004 (sql/schema.sql, rip_sessions/rip_packs/
 * pulls tables) has actually been run against the live Railway database —
 * this was never confirmed even before this file was wired in, and every
 * query here fails with "relation does not exist" if it hasn't. Confirm
 * before relying on this feature in production.
 *
 * USED BY: RipRoutes
 */

package com.poketracker.service

import com.poketracker.models.*
import com.poketracker.repository.{RipTrackerRepository, CardRepository, CollectionRepository}
import zio.*
import java.util.UUID

/**
 * What recording one pull returns — the pull itself, plus flags the UI
 * uses to nudge trading away a duplicate instead of just logging it.
 *
 * @param alreadyOwned  Whether the user owned this card BEFORE this pull.
 * @param setComplete   Whether the user now owns every card in this set,
 *                      after this pull.
 */
final case class PullResult(pull: Pull, alreadyOwned: Boolean, setComplete: Boolean)

/** A session plus every pack and pull in it, for the end-of-rip review screen. */
final case class SessionRecap(session: RipSession, packs: List[RipPack], pulls: List[Pull])

trait RipTrackerService:

  /**
   * Starts a session for a product, creating one pack row per pack it contains.
   */
  def createSession(userId: String, productId: String): Task[(RipSession, List[RipPack])]

  /**
   * Records one scanned card: saves the pull, adds it to the user's
   * collection (combining with any existing count in the same
   * condition), and reports whether it was a duplicate and/or completed the set.
   */
  def recordPull(
    userId:       String,
    ripSessionId: String,
    ripPackId:    String,
    cardId:       String,
    condition:    String
  ): Task[PullResult]

  /** The full session recap for the review screen. */
  def getRecap(userId: String, ripSessionId: String): Task[Option[SessionRecap]]

  /**
   * Builds the rip-or-hold verdict for a product. Every price gap is
   * reported, never guessed.
   * @param userId          Optional — turns on the "value to you" view.
   * @param chaseThreshold  Optional single-card price used for "odds of pulling a chase hit."
   */
  def getVerdict(
    sealedProductId:      String,
    userId:               Option[String]  = None,
    chaseThreshold:       Option[Double]  = None,
    projectedSealedPrice: Option[Double]  = None,
    trials:               Int             = 10000
  ): Task[Option[Verdict]]

object RipTrackerService:

  // Depends on five collaborators — three repositories plus two other
  // services — all handed in from outside (see `layer` at the bottom).
  final class Live(
    ripRepo:        RipTrackerRepository,
    cardRepo:       CardRepository,
    collectionRepo: CollectionRepository,
    collectionSvc:  CollectionService,
    cardSvc:        CardService
  ) extends RipTrackerService:

    def createSession(userId: String, productId: String): Task[(RipSession, List[RipPack])] =
      for
        // Stops with a clear error if the product doesn't exist — a
        // session genuinely can't be created without one.
        product <- ripRepo.findSealedProduct(productId)
                     .someOrFail(RuntimeException(s"Unknown sealed product: $productId"))
        result  <- ripRepo.createSessionWithPacks(
                     UUID.randomUUID().toString, userId, productId, product.packCount
                   )
      yield result

    /**
     * Adds `delta` to the matching condition's count, or adds a new
     * condition row if this one hasn't been logged for this card yet.
     * This is the actual merge rule: same card + same condition combines
     * into one number; same card + a different condition stays separate.
     */
    private def mergeConditionQty(
      existing:  List[ConditionCount],
      condition: String,
      delta:     Int
    ): List[ConditionCount] =
      val idx = existing.indexWhere(_.condition == condition)
      if idx >= 0 then
        // Already logged in this condition — update that row's count.
        existing.updated(idx, existing(idx).copy(quantity = existing(idx).quantity + delta))
      else
        // First time in this condition — add a new row.
        existing :+ ConditionCount(condition, delta, price = None)

    private def isSetComplete(userId: String, setId: String): Task[Boolean] =
      for
        setCards    <- cardRepo.findCardsBySet(setId)
        userEntries <- collectionRepo.findByUser(userId)
        ownedIds     = userEntries.map(_.cardId).toSet
        setCardIds   = setCards.map(_.id).toSet

      // Complete only if the set actually has cards AND the user owns
      // every one of them.
      yield setCardIds.nonEmpty && setCardIds.subsetOf(ownedIds)

    def recordPull(
      userId:       String,
      ripSessionId: String,
      ripPackId:    String,
      cardId:       String,
      condition:    String
    ): Task[PullResult] =
      for
        // Resolve route session, pack, and owner together before any mutation.
        session    <- ripRepo.findOwnedSessionForPack(ripSessionId, ripPackId, userId)
                        .someOrFail(RuntimeException("Rip session or pack not found"))

        // Makes sure this card actually exists in our catalog before referencing it.
        _          <- cardSvc.ensureCached(List(cardId))
        card       <- cardRepo.findCardById(cardId)
                        .someOrFail(RuntimeException(s"Unknown card: $cardId"))
        existing   <- collectionRepo.findEntry(session.userId, cardId)

        // Was at least one copy already owned, in any condition, before this pull?
        alreadyOwnedBefore = existing.exists(_.conditions.exists(_.quantity > 0))

        merged      = mergeConditionQty(existing.map(_.conditions).getOrElse(Nil), condition, 1)
        selCond     = existing.map(_.selectedCond).getOrElse(condition)
        updated    <- collectionSvc.updateEntry(session.userId, cardId, merged, selCond)
        pullId      = UUID.randomUUID().toString
        pull       <- ripRepo.insertPull(pullId, ripPackId, cardId, condition, updated.map(_.id))
        complete   <- isSetComplete(session.userId, card.setId)
      yield PullResult(pull, alreadyOwnedBefore, complete)

    def getRecap(userId: String, ripSessionId: String): Task[Option[SessionRecap]] =
      ripRepo.findOwnedSession(ripSessionId, userId).flatMap {
        case None => ZIO.succeed(None)
        case Some(session) =>
          for
            packs <- ripRepo.findPacksForSession(ripSessionId)
            pulls <- ripRepo.findPullsForSession(ripSessionId)
          yield Some(SessionRecap(session, packs, pulls))
      }

    def getVerdict(
      sealedProductId:      String,
      userId:               Option[String],
      chaseThreshold:       Option[Double],
      projectedSealedPrice: Option[Double],
      trials:               Int
    ): Task[Option[Verdict]] =
      ripRepo.findSealedProduct(sealedProductId).flatMap {
        case None => ZIO.succeed(None)
        case Some(product) =>
          for
            inserts    <- ripRepo.findProductInserts(sealedProductId)
            guarantees <- ripRepo.findProductGuarantees(sealedProductId)
            rates      <- ripRepo.findPullRates(sealedProductId)

            // Only guarantees for a SPECIFIC card can be priced honestly —
            // a rarity-tier guarantee ("at least one Rare Holo") doesn't
            // say which exact card you'll get, and guessing an average
            // would be exactly the kind of estimate this engine refuses
            // to make. Those stay in the database but don't add to this total.
            floorItems  = inserts.filter(_.guaranteed).flatMap(i => i.cardId.map(_ -> i.quantity)) ++
                          guarantees.flatMap(g => g.cardId.map(_ -> g.quantity))

            // Every card we'll need a price for: guaranteed items plus everything in the odds table.
            neededIds   = (floorItems.map(_._1) ++ rates.map(_.cardId)).distinct
            cards      <- ZIO.foreach(neededIds)(cardRepo.findCardById)

            // Builds a cardId -> price lookup from the Near Mint price of
            // every card we found. NM is the standard reference price used
            // everywhere else in this app.
            prices      = cards.flatten.flatMap(c => c.prices.flatMap(_.nm).map(c.id -> _)).toMap

            // From here on, this method just gathers real data and hands
            // it to RipOrHoldEngine's math — see that file for what each line computes.
            floor       = RipOrHoldEngine.guaranteedFloor(floorItems, prices)
            engineRates = rates.map(r => PullRateInput(r.cardId, r.perPackProbability))
            perPack     = RipOrHoldEngine.perPackEv(engineRates, prices)
            openEv      = floor.total + product.packCount * perPack.expectedValue
            ceiling     = RipOrHoldEngine.ceilingCheck(engineRates, prices, product.currentSealedPrice.getOrElse(0.0))
            dist        = RipOrHoldEngine.simulate(
                            engineRates, prices, product.packCount, floor.total,
                            product.currentSealedPrice.getOrElse(0.0), chaseThreshold, trials
                          )

            // Only builds the "value to you" view if we know which user is asking.
            marginal   <- userId match
                            case None => ZIO.succeed(None)
                            case Some(uid) =>
                              collectionRepo.findByUser(uid).map { entries =>
                                val owned = entries.map(_.cardId).toSet
                                Some(RipOrHoldEngine.marginalValue(
                                  engineRates, prices, owned, floor.total, product.packCount, ownedDiscount = 0.2
                                ))
                              }
          yield Some(Verdict(
            guaranteedFloor      = floor,
            perPackEv            = perPack,
            openExpectedValue    = openEv,
            distribution         = dist,
            ceiling              = ceiling,
            currentSealedPrice   = product.currentSealedPrice.getOrElse(0.0),
            projectedSealedPrice = projectedSealedPrice,
            marginalValue        = marginal
          ))
      }

  val layer: ZLayer[
    RipTrackerRepository & CardRepository & CollectionRepository & CollectionService & CardService,
    Nothing, RipTrackerService
  ] = ZLayer.fromFunction(new Live(_, _, _, _, _))
