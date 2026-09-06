/**
 * ShelfService — merges storage boxes and binders into one ordered list.
 *
 * HOW IT WORKS
 *   Goes through StorageService/BinderService for the actual box/binder
 *   data (not their repositories directly) so this stays a pure merge —
 *   any validation or business rules those services apply still apply.
 *   Self-heals: any box or binder without a shelf_items row yet (created
 *   before this feature existed, or some other gap) gets one appended
 *   on the next read, rather than silently vanishing from the shelf.
 *
 * USED BY: ShelfRoutes
 * DEPENDS ON: StorageService, BinderService, ShelfRepository
 */

package com.poketracker.service

import com.poketracker.models.*
import com.poketracker.repository.ShelfRepository
import zio.*

trait ShelfService:
  def getShelf(userId: String): Task[List[ShelfEntry]]
  def reorder(userId: String, items: List[ShelfItem]): Task[Unit]

object ShelfService:

  final class Live(
    storage: StorageService,
    binders: BinderService,
    repo:    ShelfRepository
  ) extends ShelfService:

    def getShelf(userId: String): Task[List[ShelfEntry]] =
      for
        boxes      <- storage.getBoxes(userId)
        binderList <- binders.getBinders(userId)
        _          <- ZIO.foreachDiscard(boxes)(b => repo.ensureExists(userId, "box", b.id))
        _          <- ZIO.foreachDiscard(binderList)(b => repo.ensureExists(userId, "binder", b.id))
        positions  <- repo.getPositions(userId)
        boxById     = boxes.map(b => b.id -> b).toMap
        binderById  = binderList.map(b => b.id -> b).toMap
      yield positions.flatMap { p =>
        p.kind match
          case "box" =>
            boxById.get(p.refId).map(b => ShelfEntry(kind = "box", position = p.position, box = Some(b)))
          case "binder" =>
            binderById.get(p.refId).map(b =>
              ShelfEntry(kind = "binder", position = p.position, binder = Some(BinderSummary.from(b)))
            )
          case _ => None
      }

    def reorder(userId: String, items: List[ShelfItem]): Task[Unit] = repo.reorder(userId, items)

  val layer: ZLayer[StorageService & BinderService & ShelfRepository, Nothing, ShelfService] =
    ZLayer.fromFunction(new Live(_, _, _))
