/**
 * StorageService — physical storage: boxes, drawers, assigning owned
 * cards to them.
 *
 * HOW IT WORKS
 *   Thin rules layer over StorageRepository. Most methods are direct
 *   pass-throughs; the one with real logic is assignCards, which flags
 *   (but never blocks) when a set is being split across drawers.
 *   createBox/deleteBox also keep ShelfRepository's ordering table in sync.
 *
 * USED BY: StorageRoutes
 * DEPENDS ON: StorageRepository, ShelfRepository
 */

package com.poketracker.service

import com.poketracker.models.*
import com.poketracker.repository.{ShelfRepository, StorageRepository}
import zio.*

trait StorageService:

  def getBoxes(userId: String): Task[List[StorageBox]]

  /** kind "display_case" gets exactly one drawer created for it automatically —
   *  see the class doc. Plain boxes ("box") start with none; the caller adds
   *  as many as they want via createDrawer. */
  def createBox(userId: String, name: String, kind: String, boxType: String, capacity: Int, color: String): Task[StorageBox]
  def renameBox(userId: String, id: String, name: String): Task[Unit]
  def reorderBox(userId: String, id: String, position: Int): Task[Unit]
  def deleteBox(userId: String, id: String): Task[Unit]

  def createDrawer(userId: String, boxId: String, name: String): Task[StorageDrawer]
  def renameDrawer(userId: String, id: String, name: String): Task[Unit]
  def reorderDrawer(userId: String, id: String, position: Int): Task[Unit]
  def deleteDrawer(userId: String, id: String): Task[Unit]

  def getDrawerCards(userId: String, drawerId: String): Task[List[OwnedCard]]
  def getUnassignedCards(userId: String): Task[List[OwnedCard]]

  /**
   * Assigns cards to a drawer, then checks whether any OTHER owned card
   * from the same set already lives in a different drawer. Surfaced as an
   * informational warning only — splitting a set across boxes is a real
   * thing users do, so this never blocks the assignment.
   */
  def assignCards(userId: String, cardIds: List[String], drawerId: String): Task[AssignResult]

  def unassignCard(userId: String, cardId: String): Task[Unit]

object StorageService:

  private def buildOverlapWarning(overlap: Int): Option[String] =
    if overlap == 0 then None
    else
      val plural = if overlap == 1 then "" else "s"
      val verb   = if overlap == 1 then "is" else "are"
      Some(s"$overlap other card$plural from the same set(s) $verb already in a different drawer.")

  final class Live(repo: StorageRepository, shelf: ShelfRepository) extends StorageService:

    def getBoxes(userId: String): Task[List[StorageBox]] = repo.findBoxesByUser(userId)

    def createBox(userId: String, name: String, kind: String, boxType: String, capacity: Int, color: String): Task[StorageBox] =
      for
        _      <- ZIO.when(name.trim.isEmpty)(ZIO.fail(new IllegalArgumentException("Box name cannot be empty")))
        _      <- ZIO.when(capacity < 0)(ZIO.fail(new IllegalArgumentException("Capacity cannot be negative")))
        box    <- repo.createBox(userId, name.trim, kind, boxType, capacity, color)
        _      <- shelf.ensureExists(userId, "box", box.id)
        drawer <- ZIO.when(kind == "display_case")(repo.createDrawer(userId, box.id, "Display"))
      yield box.copy(drawers = drawer.toList)

    def renameBox(userId: String, id: String, name: String): Task[Unit] =
      ZIO.when(name.trim.isEmpty)(ZIO.fail(new IllegalArgumentException("Box name cannot be empty")))
        *> repo.renameBox(userId, id, name.trim)

    def reorderBox(userId: String, id: String, position: Int): Task[Unit] = repo.reorderBox(userId, id, position)
    def deleteBox(userId: String, id: String): Task[Unit] =
      repo.deleteBox(userId, id) *> shelf.remove(userId, "box", id)

    def createDrawer(userId: String, boxId: String, name: String): Task[StorageDrawer] =
      ZIO.when(name.trim.isEmpty)(ZIO.fail(new IllegalArgumentException("Drawer name cannot be empty")))
        *> repo.createDrawer(userId, boxId, name.trim)

    def renameDrawer(userId: String, id: String, name: String): Task[Unit] =
      ZIO.when(name.trim.isEmpty)(ZIO.fail(new IllegalArgumentException("Drawer name cannot be empty")))
        *> repo.renameDrawer(userId, id, name.trim)

    def reorderDrawer(userId: String, id: String, position: Int): Task[Unit] = repo.reorderDrawer(userId, id, position)
    def deleteDrawer(userId: String, id: String): Task[Unit] = repo.deleteDrawer(userId, id)

    def getDrawerCards(userId: String, drawerId: String): Task[List[OwnedCard]] =
      repo.findDrawerCards(userId, drawerId)
    def getUnassignedCards(userId: String): Task[List[OwnedCard]] = repo.findUnassignedCards(userId)

    def assignCards(userId: String, cardIds: List[String], drawerId: String): Task[AssignResult] =
      for
        assigned <- repo.assignCards(userId, cardIds, drawerId)
        // Runs after the assignment, not before — needs to see the cards just moved.
        overlap  <- repo.countOtherDrawerCardsInSameSets(userId, cardIds, drawerId)
        warning   = buildOverlapWarning(overlap)
      yield AssignResult(assigned, warning)

    def unassignCard(userId: String, cardId: String): Task[Unit] = repo.unassignCard(userId, cardId)

  val layer: ZLayer[StorageRepository & ShelfRepository, Nothing, StorageService] =
    ZLayer.fromFunction(new Live(_, _))
