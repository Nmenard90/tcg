/**
 * StorageRepository — all database access for physical storage: boxes,
 * drawers, and which drawer an owned card is filed into.
 *
 * HOW IT WORKS: A card's location lives directly on its collection_entries
 * row (`drawer_id`), not a separate assignment table — there's already one
 * row per (user, card) and this app tracks one location per card, not per
 * physical copy.
 *
 * USED BY: StorageService
 */

package com.poketracker.repository

import cats.syntax.all.*
import com.poketracker.models.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import doobie.util.transactor.Transactor
import zio.*
import zio.interop.catz.*
import zio.json.*
import java.time.Instant

trait StorageRepository:
  def findBoxesByUser(userId: String): Task[List[StorageBox]]
  def createBox(userId: String, name: String, kind: String, boxType: String, capacity: Int, color: String): Task[StorageBox]
  def renameBox(userId: String, id: String, name: String): Task[Unit]
  def reorderBox(userId: String, id: String, position: Int): Task[Unit]

  /** Cards in this box's drawers become unassigned, not deleted. */
  def deleteBox(userId: String, id: String): Task[Unit]

  def createDrawer(userId: String, boxId: String, name: String): Task[StorageDrawer]
  def renameDrawer(userId: String, id: String, name: String): Task[Unit]
  def reorderDrawer(userId: String, id: String, position: Int): Task[Unit]

  /** Cards in this drawer become unassigned, not deleted. */
  def deleteDrawer(userId: String, id: String): Task[Unit]

  def findDrawerCards(userId: String, drawerId: String): Task[List[OwnedCard]]
  def findUnassignedCards(userId: String): Task[List[OwnedCard]]

  /** @return how many cards were actually moved. */
  def assignCards(userId: String, cardIds: List[String], drawerId: String): Task[Int]

  /**
   * Counts owned cards from the same set(s) already filed in a DIFFERENT
   * drawer — purely informational, feeds a "this set is split across boxes"
   * heads-up. Call after assignCards: the cards just moved already carry
   * the new drawerId, so they're naturally excluded by the different-drawer check.
   */
  def countOtherDrawerCardsInSameSets(userId: String, cardIds: List[String], drawerId: String): Task[Int]

  def unassignCard(userId: String, cardId: String): Task[Unit]

object StorageRepository:

  final class Live(xa: Transactor[Task]) extends StorageRepository:

    def findBoxesByUser(userId: String): Task[List[StorageBox]] =
      val boxesQuery =
        sql"""
          SELECT id, user_id, name, kind, box_type, capacity, color, position,
                 space_id, storage_unit_id, shelf_index, stack_index, stack_level,
                 created_at, updated_at
          FROM storage_boxes
          WHERE user_id = $userId
          ORDER BY position, created_at
        """
          .query[(String, String, String, String, String, Int, String, Int,
                  Option[String], Option[String], Option[Int], Option[Int], Option[Int], Instant, Instant)]
          .to[List]

      val drawersQuery =
        sql"""
          SELECT d.id, d.box_id, d.name, d.position, COUNT(ce.card_id), d.created_at, d.updated_at
          FROM storage_drawers d
          JOIN storage_boxes b ON b.id = d.box_id
          LEFT JOIN collection_entries ce ON ce.drawer_id = d.id
          WHERE b.user_id = $userId
          GROUP BY d.id
          ORDER BY d.box_id, d.position, d.created_at
        """
          .query[(String, String, String, Int, Int, Instant, Instant)]
          .to[List]

      (for
        boxes   <- boxesQuery
        drawers <- drawersQuery
      yield
        val drawersByBox = drawers
          .map { case (id, boxId, name, pos, count, createdAt, updatedAt) =>
            StorageDrawer(id, boxId, name, pos, count, createdAt, updatedAt)
          }
          .groupBy(_.boxId)

        boxes.map { case (id, uid, name, kind, boxType, capacity, color, pos, spaceId, unitId, shelf, stack, level, createdAt, updatedAt) =>
          StorageBox(id, uid, name, kind, boxType, capacity, color, pos, spaceId, unitId, shelf, stack, level, drawersByBox.getOrElse(id, Nil), createdAt, updatedAt)
        }
      ).transact(xa)

    def createBox(userId: String, name: String, kind: String, boxType: String, capacity: Int, color: String): Task[StorageBox] =
      for
        nextPos <- sql"SELECT COALESCE(MAX(position) + 1, 0) FROM storage_boxes WHERE user_id = $userId"
                     .query[Int].unique.transact(xa)
        id       = java.util.UUID.randomUUID().toString
        now      = Instant.now()
        _       <- sql"""
                     INSERT INTO storage_boxes (id, user_id, name, kind, box_type, capacity, color, position, created_at, updated_at)
                     VALUES ($id, $userId, $name, $kind, $boxType, $capacity, $color, $nextPos, $now, $now)
                   """.update.run.void.transact(xa)
      yield StorageBox(id, userId, name, kind, boxType, capacity, color, nextPos, None, None, None, None, None, Nil, now, now)

    def renameBox(userId: String, id: String, name: String): Task[Unit] =
      sql"UPDATE storage_boxes SET name = $name, updated_at = NOW() WHERE id = $id AND user_id = $userId"
        .update.run.void.transact(xa)

    def reorderBox(userId: String, id: String, position: Int): Task[Unit] =
      sql"UPDATE storage_boxes SET position = $position, updated_at = NOW() WHERE id = $id AND user_id = $userId"
        .update.run.void.transact(xa)

    def deleteBox(userId: String, id: String): Task[Unit] =
      sql"DELETE FROM storage_boxes WHERE id = $id AND user_id = $userId".update.run.void.transact(xa)

    def createDrawer(userId: String, boxId: String, name: String): Task[StorageDrawer] =
      val id  = java.util.UUID.randomUUID().toString
      val now = Instant.now()
      sql"""
        WITH next_position AS (
          SELECT COALESCE(MAX(d.position) + 1, 0) AS position
          FROM storage_boxes b
          LEFT JOIN storage_drawers d ON d.box_id = b.id
          WHERE b.id = $boxId AND b.user_id = $userId
          GROUP BY b.id
        )
        INSERT INTO storage_drawers (id, box_id, name, position, created_at, updated_at)
        SELECT $id, $boxId, $name, position, $now, $now FROM next_position
        RETURNING position
      """.query[Int].option.transact(xa)
        .someOrFail(new IllegalArgumentException("Box not found"))
        .map(nextPos => StorageDrawer(id, boxId, name, nextPos, 0, now, now))

    def renameDrawer(userId: String, id: String, name: String): Task[Unit] =
      sql"""
        UPDATE storage_drawers d SET name = $name, updated_at = NOW()
        FROM storage_boxes b
        WHERE d.id = $id AND b.id = d.box_id AND b.user_id = $userId
      """
        .update.run.void.transact(xa)

    def reorderDrawer(userId: String, id: String, position: Int): Task[Unit] =
      sql"""
        UPDATE storage_drawers d SET position = $position, updated_at = NOW()
        FROM storage_boxes b
        WHERE d.id = $id AND b.id = d.box_id AND b.user_id = $userId
      """
        .update.run.void.transact(xa)

    def deleteDrawer(userId: String, id: String): Task[Unit] =
      sql"""
        DELETE FROM storage_drawers d
        USING storage_boxes b
        WHERE d.id = $id AND b.id = d.box_id AND b.user_id = $userId
      """.update.run.void.transact(xa)

    /**
     * Base SELECT shared by findDrawerCards/findUnassignedCards, extended
     * with a WHERE clause via `++`. Built with Fragment.const on a fixed
     * literal string (no interpolated values) specifically so it composes
     * safely with `++` and `fr"..."` fragments below — string-concatenating
     * a raw SQL string with user-supplied values here was a real injection
     * bug previously in this codebase.
     */
    private val ownedCardQuery: Fragment =
      Fragment.const(
        """
        SELECT
          ce.card_id, ce.conditions, ce.selected_cond, ce.updated_at, ce.drawer_id,
          c.id, c.set_id, c.name, c.number, c.rarity, c.artist,
          c.image_small, c.image_large,
          p.price_nm, p.price_lp, p.price_mp, p.price_hp, p.price_dmg
        FROM collection_entries ce
        JOIN cards c ON c.id = ce.card_id
        LEFT JOIN card_prices p ON p.card_id = ce.card_id
        """
      )

    private def toOwnedCards(rows: List[
      (String, String, String, Instant, Option[String],
       String, String, String, String, Option[String], Option[String],
       String, String,
       Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])
    ]): List[OwnedCard] =
      rows.flatMap {
        case (cardId, condJson, selCond, updatedAt, drawerId,
              cId, setId, name, number, rarity, artist,
              imgSmall, imgLarge,
              nm, lp, mp, hp, dmg) =>
          condJson.fromJson[List[ConditionCount]].toOption.map { conditions =>
            val prices = if nm.orElse(lp).orElse(mp).orElse(hp).orElse(dmg).isDefined
                         then Some(CardPrices(nm, lp, mp, hp, dmg))
                         else None
            OwnedCard(
              cardId       = cardId,
              conditions   = conditions,
              selectedCond = selCond,
              updatedAt    = updatedAt,
              card         = Card(cId, setId, name, number, rarity, artist,
                                  CardImage(imgSmall, imgLarge), prices, None),
              drawerId     = drawerId
            )
          }
      }

    def findDrawerCards(userId: String, drawerId: String): Task[List[OwnedCard]] =
      (ownedCardQuery ++ fr"""
        WHERE ce.user_id = $userId
          AND ce.drawer_id = $drawerId
          AND EXISTS (
            SELECT 1 FROM storage_drawers d
            JOIN storage_boxes b ON b.id = d.box_id
            WHERE d.id = $drawerId AND b.user_id = $userId
          )
        ORDER BY ce.updated_at DESC
      """)
        .query[(String, String, String, Instant, Option[String],
                String, String, String, String, Option[String], Option[String],
                String, String,
                Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])]
        .to[List]
        .map(toOwnedCards)
        .transact(xa)

    def findUnassignedCards(userId: String): Task[List[OwnedCard]] =
      (ownedCardQuery ++ fr"WHERE ce.user_id = $userId AND ce.drawer_id IS NULL ORDER BY ce.updated_at DESC")
        .query[(String, String, String, Instant, Option[String],
                String, String, String, String, Option[String], Option[String],
                String, String,
                Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])]
        .to[List]
        .map(toOwnedCards)
        .transact(xa)

    def assignCards(userId: String, cardIds: List[String], drawerId: String): Task[Int] =
      cardIds.traverse { cardId =>
        sql"""
          UPDATE collection_entries ce SET drawer_id = $drawerId
          WHERE ce.user_id = $userId AND ce.card_id = $cardId
            AND EXISTS (
              SELECT 1 FROM storage_drawers d
              JOIN storage_boxes b ON b.id = d.box_id
              WHERE d.id = $drawerId AND b.user_id = $userId
            )
        """.update.run
      }.map(_.sum).transact(xa)

    def countOtherDrawerCardsInSameSets(userId: String, cardIds: List[String], drawerId: String): Task[Int] =
      if cardIds.isEmpty then ZIO.succeed(0)
      else
        val idMatch = cardIds.map(id => fr"c2.id = $id").reduce(_ ++ fr" OR " ++ _)

        (fr"""
          SELECT COUNT(*) FROM collection_entries ce
          JOIN cards c ON c.id = ce.card_id
          WHERE ce.user_id = $userId
            AND ce.drawer_id IS NOT NULL
            AND ce.drawer_id != $drawerId
            AND c.set_id IN (
              SELECT DISTINCT c2.set_id FROM cards c2 WHERE (""" ++ idMatch ++ fr"))")
          .query[Int].unique.transact(xa)

    def unassignCard(userId: String, cardId: String): Task[Unit] =
      sql"""
        UPDATE collection_entries SET drawer_id = NULL
        WHERE user_id = $userId AND card_id = $cardId
      """.update.run.void.transact(xa)

  val layer: ZLayer[Transactor[Task], Nothing, StorageRepository] =
    ZLayer.fromFunction(new Live(_))
