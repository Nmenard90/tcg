/**
 * RipTrackerRepository — all database access for the Rip Tracker feature
 * (sealed products, their guaranteed contents/pull rates, and box-opening
 * sessions/packs/pulls). Migration 004 in sql/schema.sql.
 *
 * WIRED INTO Main.scala via RipTrackerService/RipRoutes.
 * UNVERIFIED: whether Migration 004 has actually been run against the live
 * Railway database — see AGENTS.md's migration-before-deploy rule. Every
 * query here fails with "relation does not exist" if it hasn't. This was
 * never confirmed even before this repository was wired in; confirm the
 * migration has run before relying on this feature in production.
 *
 * USED BY: RipTrackerService
 */

package com.poketracker.repository

import com.poketracker.models.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import doobie.util.transactor.Transactor
import zio.*
import zio.interop.catz.*
import zio.json.*
import java.time.Instant

trait RipTrackerRepository:

  def findSealedProduct(id: String): Task[Option[SealedProduct]]
  def findSealedProductsForSet(setId: String): Task[List[SealedProduct]]
  def findPackTemplates(sealedProductId: String): Task[List[PackTemplate]]
  def findProductInserts(sealedProductId: String): Task[List[ProductInsert]]

  /** Empty for ungoverned (has_guarantee=false) products. */
  def findProductGuarantees(sealedProductId: String): Task[List[ProductGuarantee]]

  def findPullRates(sealedProductId: String): Task[List[PullRate]]

  /**
   * Creates a session and its `packCount` packs as one transaction.
   * @param id  Caller-generated so it can be returned immediately without a round trip.
   */
  def createSessionWithPacks(
    id:              String,
    userId:          String,
    sealedProductId: String,
    packCount:       Int
  ): Task[(RipSession, List[RipPack])]

  /** Finds a session only when it belongs to the authenticated user. */
  def findOwnedSession(id: String, userId: String): Task[Option[RipSession]]

  /** Resolves a pack through the named session and authenticated owner. */
  def findOwnedSessionForPack(
    ripSessionId: String,
    ripPackId:    String,
    userId:       String
  ): Task[Option[RipSession]]

  def findPacksForSession(ripSessionId: String): Task[List[RipPack]]
  def findPullsForSession(ripSessionId: String): Task[List[Pull]]

  def insertPull(
    id:                String,
    ripPackId:         String,
    cardId:            String,
    condition:         String,
    collectionEntryId: Option[String]
  ): Task[Pull]

object RipTrackerRepository:

  final class Live(xa: Transactor[Task]) extends RipTrackerRepository:

    /** Malformed JSON degrades to an empty slot list rather than failing the whole query. */
    private def parseSlots(json: String): List[PackSlot] =
      json.fromJson[List[PackSlot]].getOrElse(Nil)

    def findSealedProduct(id: String): Task[Option[SealedProduct]] =
      sql"""
        SELECT id, name, set_id, kind, pack_count, has_guarantee,
               current_sealed_price, sealed_price_source, sealed_price_updated_at,
               created_at, updated_at
        FROM sealed_products
        WHERE id = $id
      """
        .query[(String, String, String, String, Int, Boolean,
                Option[Double], Option[String], Option[Instant], Instant, Instant)]
        .option
        .map(_.map(SealedProduct.apply.tupled))
        .transact(xa)

    def findSealedProductsForSet(setId: String): Task[List[SealedProduct]] =
      sql"""
        SELECT id, name, set_id, kind, pack_count, has_guarantee,
               current_sealed_price, sealed_price_source, sealed_price_updated_at,
               created_at, updated_at
        FROM sealed_products
        WHERE set_id = $setId
        ORDER BY name
      """
        .query[(String, String, String, String, Int, Boolean,
                Option[Double], Option[String], Option[Instant], Instant, Instant)]
        .to[List]
        .map(_.map(SealedProduct.apply.tupled))
        .transact(xa)

    def findPackTemplates(sealedProductId: String): Task[List[PackTemplate]] =
      sql"""
        SELECT id, sealed_product_id, name, slots::text, created_at
        FROM pack_templates
        WHERE sealed_product_id = $sealedProductId
      """
        .query[(String, String, String, String, Instant)]
        .to[List]
        .map(_.map { case (id, pid, name, slotsJson, createdAt) =>
          PackTemplate(id, pid, name, parseSlots(slotsJson), createdAt)
        })
        .transact(xa)

    def findProductInserts(sealedProductId: String): Task[List[ProductInsert]] =
      sql"""
        SELECT id, sealed_product_id, card_id, description, guaranteed, quantity
        FROM product_inserts
        WHERE sealed_product_id = $sealedProductId
      """
        .query[(String, String, Option[String], Option[String], Boolean, Int)]
        .to[List]
        .map(_.map(ProductInsert.apply.tupled))
        .transact(xa)

    def findProductGuarantees(sealedProductId: String): Task[List[ProductGuarantee]] =
      sql"""
        SELECT id, sealed_product_id, rarity, card_id, quantity
        FROM product_guarantees
        WHERE sealed_product_id = $sealedProductId
      """
        .query[(String, String, Option[String], Option[String], Int)]
        .to[List]
        .map(_.map(ProductGuarantee.apply.tupled))
        .transact(xa)

    def findPullRates(sealedProductId: String): Task[List[PullRate]] =
      sql"""
        SELECT id, card_id, sealed_product_id, per_pack_probability, source, sample_size, created_at
        FROM pull_rates
        WHERE sealed_product_id = $sealedProductId
      """
        .query[(String, String, String, Double, String, Option[Int], Instant)]
        .to[List]
        .map(_.map(PullRate.apply.tupled))
        .transact(xa)

    def createSessionWithPacks(
      id:              String,
      userId:          String,
      sealedProductId: String,
      packCount:       Int
    ): Task[(RipSession, List[RipPack])] =
      val now = Instant.now()

      val insertSession = sql"""
        INSERT INTO rip_sessions (id, user_id, sealed_product_id, status, created_at)
        VALUES ($id, $userId, $sealedProductId, 'in_progress', $now)
      """.update.run

      // Generated here, not by the caller, so packCount stays the single source of truth.
      val packIds = List.fill(packCount)(java.util.UUID.randomUUID().toString)
      val insertPacks = (packIds.zipWithIndex).traverse_ { case (packId, idx) =>
        sql"""
          INSERT INTO rip_packs (id, rip_session_id, pack_index)
          VALUES ($packId, $id, $idx)
        """.update.run
      }

      // Both inserts as one transaction; we already know what we wrote,
      // so build the return value directly rather than reading it back.
      (insertSession *> insertPacks).transact(xa).as(
        (
          RipSession(id, userId, sealedProductId, RipSessionStatus.InProgress, now),
          packIds.zipWithIndex.map { case (packId, idx) => RipPack(packId, id, idx) }
        )
      )

    def findOwnedSession(id: String, userId: String): Task[Option[RipSession]] =
      sql"""
        SELECT id, user_id, sealed_product_id, status, created_at
        FROM rip_sessions
        WHERE id = $id AND user_id = $userId
      """
        .query[(String, String, String, String, Instant)]
        .option
        .map(_.map { case (id, userId, productId, status, createdAt) =>
          val parsedStatus = if status == "completed" then RipSessionStatus.Completed else RipSessionStatus.InProgress
          RipSession(id, userId, productId, parsedStatus, createdAt)
        })
        .transact(xa)

    def findOwnedSessionForPack(
      ripSessionId: String,
      ripPackId:    String,
      userId:       String
    ): Task[Option[RipSession]] =
      sql"""
        SELECT rs.id, rs.user_id, rs.sealed_product_id, rs.status, rs.created_at
        FROM rip_sessions rs
        JOIN rip_packs rp ON rp.rip_session_id = rs.id
        WHERE rs.id = $ripSessionId
          AND rs.user_id = $userId
          AND rp.id = $ripPackId
      """
        .query[(String, String, String, String, Instant)]
        .option
        .map(_.map { case (id, ownerId, productId, status, createdAt) =>
          val parsedStatus = if status == "completed" then RipSessionStatus.Completed else RipSessionStatus.InProgress
          RipSession(id, ownerId, productId, parsedStatus, createdAt)
        })
        .transact(xa)

    def findPacksForSession(ripSessionId: String): Task[List[RipPack]] =
      sql"""
        SELECT id, rip_session_id, pack_index
        FROM rip_packs
        WHERE rip_session_id = $ripSessionId
        ORDER BY pack_index
      """
        .query[(String, String, Int)]
        .to[List]
        .map(_.map(RipPack.apply.tupled))
        .transact(xa)

    def findPullsForSession(ripSessionId: String): Task[List[Pull]] =
      sql"""
        SELECT pl.id, pl.rip_pack_id, pl.card_id, pl.condition, pl.collection_entry_id, pl.created_at
        FROM pulls pl
        JOIN rip_packs pk ON pk.id = pl.rip_pack_id
        WHERE pk.rip_session_id = $ripSessionId
        ORDER BY pl.created_at DESC
      """
        .query[(String, String, String, String, Option[String], Instant)]
        .to[List]
        .map(_.map { case (id, packId, cardId, cond, entryId, createdAt) =>
          Pull(id, packId, cardId, cond, entryId, createdAt)
        })
        .transact(xa)

    def insertPull(
      id:                String,
      ripPackId:         String,
      cardId:            String,
      condition:         String,
      collectionEntryId: Option[String]
    ): Task[Pull] =
      val now = Instant.now()
      sql"""
        INSERT INTO pulls (id, rip_pack_id, card_id, condition, collection_entry_id, created_at)
        VALUES ($id, $ripPackId, $cardId, $condition, $collectionEntryId, $now)
      """.update.run.transact(xa)
        .as(Pull(id, ripPackId, cardId, condition, collectionEntryId, now))

  val layer: ZLayer[Transactor[Task], Nothing, RipTrackerRepository] =
    ZLayer.fromFunction(new Live(_))
