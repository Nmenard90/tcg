/**
 * BinderRepository — all database access for binders and their card slots.
 *
 * HOW IT WORKS
 *   Binders and slots live in two tables (`binders`, `binder_slots`).
 *   `findByUser` returns binder covers only (no slots — the shelf view
 *   doesn't need them). `findById` and `updateSlot` each run two
 *   statements as one transaction so a binder's slots can never be read
 *   or written half-updated relative to the binder row itself.
 *
 * USED BY: BinderService
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
import java.time.Instant

trait BinderRepository:
  /** Slots omitted — used for the shelf view, which only shows covers. */
  def findByUser(userId: String): Task[List[Binder]]

  /** Full binder with every slot — used when a binder is actually opened. */
  def findById(userId: String, id: String): Task[Option[Binder]]

  def create(binder: Binder): Task[Unit]

  /** `cardId = None` clears the slot; `Some` places that card. */
  def updateSlot(
    userId:    String,
    binderId:  String,
    slotIndex: Int,
    cardId:    Option[String],
    cardName:  Option[String],
    imageUrl:  Option[String]
  ): Task[Unit]

  def updateName(userId: String, id: String, name: String): Task[Unit]
  def updateCover(userId: String, id: String, imageUrl: String): Task[Unit]

  /** Slot indexes are untouched — pages simply re-flow to the new pockets-per-page. */
  def updatePocketSize(userId: String, id: String, pocketSize: PocketSize): Task[Unit]

  /** Slots cascade-delete via the schema's FK constraint. */
  def delete(userId: String, id: String): Task[Unit]

object BinderRepository:

  final class Live(xa: Transactor[Task]) extends BinderRepository:

    def findByUser(userId: String): Task[List[Binder]] =
      sql"""
        SELECT id, user_id, name, pocket_size, cover_image, space_id, storage_unit_id,
               shelf_index, shelf_position, created_at, updated_at
        FROM binders
        WHERE user_id = $userId
        ORDER BY updated_at DESC
      """
        .query[(String, String, String, String, Option[String], Option[String], Option[String], Option[Int], Option[Int], Instant, Instant)]
        .to[List]
        .map(_.map { case (id, uid, name, size, cover, spaceId, unitId, shelf, position, createdAt, updatedAt) =>
          Binder(id, uid, name, PocketSize.valueOf(size), cover, spaceId, unitId, shelf, position, Nil, createdAt, updatedAt)
        })
        .transact(xa)

    def findById(userId: String, id: String): Task[Option[Binder]] =
      val binderQuery =
        sql"""
          SELECT id, user_id, name, pocket_size, cover_image, space_id, storage_unit_id,
                 shelf_index, shelf_position, created_at, updated_at
          FROM binders WHERE id = $id AND user_id = $userId
        """
          .query[(String, String, String, String, Option[String], Option[String], Option[String], Option[Int], Option[Int], Instant, Instant)]
          .option

      val slotsQuery =
        sql"""
          SELECT slot_index, card_id, card_name, image_url
          FROM binder_slots bs
          JOIN binders b ON b.id = bs.binder_id
          WHERE bs.binder_id = $id AND b.user_id = $userId
          ORDER BY slot_index
        """
          .query[(Int, Option[String], Option[String], Option[String])]
          .to[List]

      // Both queries run in the same transaction so slots can't be read
      // against a binder row that changed in between.
      (for
        binderOpt <- binderQuery
        slots     <- slotsQuery
      yield binderOpt.map { case (bid, uid, name, size, cover, spaceId, unitId, shelf, position, createdAt, updatedAt) =>
        val binderSlots = slots.map { case (idx, cardId, cardName, imgUrl) =>
          BinderSlot(idx, cardId, cardName, imgUrl)
        }
        Binder(bid, uid, name, PocketSize.valueOf(size), cover, spaceId, unitId, shelf, position, binderSlots, createdAt, updatedAt)
      }).transact(xa)

    def create(binder: Binder): Task[Unit] =
      sql"""
        INSERT INTO binders (id, user_id, name, pocket_size, cover_image, created_at, updated_at)
        VALUES (${binder.id}, ${binder.userId}, ${binder.name},
                ${binder.pocketSize.toString}, ${binder.coverImage},
                ${binder.createdAt}, ${binder.updatedAt})
      """
        .update.run.void.transact(xa)

    def updateSlot(
      userId:    String,
      binderId:  String,
      slotIndex: Int,
      cardId:    Option[String],
      cardName:  Option[String],
      imageUrl:  Option[String]
    ): Task[Unit] =
      sql"""
        INSERT INTO binder_slots (binder_id, slot_index, card_id, card_name, image_url)
        SELECT b.id, $slotIndex, $cardId, $cardName, $imageUrl
        FROM binders b
        WHERE b.id = $binderId AND b.user_id = $userId
        ON CONFLICT (binder_id, slot_index) DO UPDATE SET
          card_id   = EXCLUDED.card_id,
          card_name = EXCLUDED.card_name,
          image_url = EXCLUDED.image_url
      """
        .update.run.void
        // Bumps the binder's own updated_at in the same transaction, so
        // the shelf sorts by most-recently-touched binder.
        .flatMap { _ =>
          sql"UPDATE binders SET updated_at = NOW() WHERE id = $binderId AND user_id = $userId"
            .update.run.void
        }
        .transact(xa)

    def updateName(userId: String, id: String, name: String): Task[Unit] =
      sql"""
        UPDATE binders SET name = $name, updated_at = NOW() WHERE id = $id AND user_id = $userId
      """
        .update.run.void.transact(xa)

    def updateCover(userId: String, id: String, imageUrl: String): Task[Unit] =
      sql"""
        UPDATE binders SET cover_image = $imageUrl, updated_at = NOW() WHERE id = $id AND user_id = $userId
      """
        .update.run.void.transact(xa)

    def updatePocketSize(userId: String, id: String, pocketSize: PocketSize): Task[Unit] =
      sql"""
        UPDATE binders SET pocket_size = ${pocketSize.toString}, updated_at = NOW()
        WHERE id = $id AND user_id = $userId
      """
        .update.run.void.transact(xa)

    def delete(userId: String, id: String): Task[Unit] =
      sql"DELETE FROM binders WHERE id = $id AND user_id = $userId"
        .update.run.void.transact(xa)

  val layer: ZLayer[Transactor[Task], Nothing, BinderRepository] =
    ZLayer.fromFunction(new Live(_))
