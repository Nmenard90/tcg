/**
 * ShelfRepository — all database access for the shelf_items ordering table.
 * Knows nothing about what a box or binder actually is, only that one
 * exists at (kind, refId) and has a position.
 *
 * USED BY: ShelfService
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

trait ShelfRepository:
  def getPositions(userId: String): Task[List[ShelfItem]]

  /** Inserts a row at the end if (kind, refId) doesn't already have one —
   *  a no-op otherwise. Used both right after creating a box/binder and as
   *  a self-heal for any item that predates this table. */
  def ensureExists(userId: String, kind: String, refId: String): Task[Unit]

  def remove(userId: String, kind: String, refId: String): Task[Unit]

  /** Client sends the whole new order; this just writes the positions —
   *  same shape as StorageRepository.reorderBox, one row at a time. */
  def reorder(userId: String, items: List[ShelfItem]): Task[Unit]

object ShelfRepository:

  final class Live(xa: Transactor[Task]) extends ShelfRepository:

    def getPositions(userId: String): Task[List[ShelfItem]] =
      sql"""
        SELECT kind, ref_id, position
        FROM shelf_items
        WHERE user_id = $userId
        ORDER BY position, created_at
      """
        .query[(String, String, Int)]
        .to[List]
        .map(_.map { case (kind, refId, pos) => ShelfItem(kind, refId, pos) })
        .transact(xa)

    def ensureExists(userId: String, kind: String, refId: String): Task[Unit] =
      val id = java.util.UUID.randomUUID().toString
      (for
        nextPos <- sql"SELECT COALESCE(MAX(position) + 1, 0) FROM shelf_items WHERE user_id = $userId"
                     .query[Int].unique
        // The polymorphic reference is admitted only when the target belongs
        // to this same user. This keeps even internal callers fail-closed.
        _ <- sql"""
               INSERT INTO shelf_items (id, user_id, kind, ref_id, position)
               SELECT $id, $userId, $kind, $refId, $nextPos
               WHERE ($kind = 'box' AND EXISTS (
                        SELECT 1 FROM storage_boxes WHERE id = $refId AND user_id = $userId
                      ))
                  OR ($kind = 'binder' AND EXISTS (
                        SELECT 1 FROM binders WHERE id = $refId AND user_id = $userId
                      ))
               ON CONFLICT (kind, ref_id) DO NOTHING
             """.update.run
      yield ()).transact(xa)

    def remove(userId: String, kind: String, refId: String): Task[Unit] =
      sql"DELETE FROM shelf_items WHERE user_id = $userId AND kind = $kind AND ref_id = $refId"
        .update.run.void.transact(xa)

    def reorder(userId: String, items: List[ShelfItem]): Task[Unit] =
      items.traverse { item =>
        sql"""
          UPDATE shelf_items SET position = ${item.position}
          WHERE user_id = $userId AND kind = ${item.kind} AND ref_id = ${item.refId}
        """.update.run
      }.void.transact(xa)

  val layer: ZLayer[Transactor[Task], Nothing, ShelfRepository] =
    ZLayer.fromFunction(new Live(_))
