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

trait RoomRepository:
  def listSpaces(userId: String): Task[List[CollectionSpace]]
  def createSpace(userId: String, name: String, spaceType: String): Task[CollectionSpace]
  def createStorageUnit(userId: String, spaceId: String, name: String, unitType: String,
    preset: String, color: String, shelfCount: Int, positionsPerShelf: Int,
    maxStackHeight: Int, config: String): Task[StorageUnit]
  def placeBox(userId: String, boxId: String, spaceId: String, unitId: String,
    shelfIndex: Int, stackIndex: Int, stackLevel: Int): Task[Unit]
  def placeBinder(userId: String, binderId: String, spaceId: String, unitId: String,
    shelfIndex: Int, shelfPosition: Int): Task[Unit]
  def createDisplayCase(userId: String, spaceId: String, name: String, caseType: String,
    preset: String, frameColor: String, lightColor: String, shelfCount: Int,
    slotsPerShelf: Int, config: String): Task[DisplayCase]
  def setDisplayLights(userId: String, caseId: String, enabled: Boolean): Task[Unit]
  def listLots(userId: String): Task[List[InventoryLot]]
  def listDrawerAllocations(userId: String, drawerId: String): Task[List[CardAllocation]]
  def listCaseAllocations(userId: String, caseId: String): Task[List[CardAllocation]]
  def listBinderAllocations(userId: String, binderId: String): Task[List[CardAllocation]]
  def allocate(userId: String, lotId: String, drawerId: Option[String],
    binderSlotId: Option[String], displaySlotId: Option[String], quantity: Int,
    protection: Option[String], notes: Option[String]): Task[CardAllocation]
  /** Binder slots are lazily created (unlike display slots, which are all
   *  pre-created with the case) — the first placement at a given index
   *  creates its `binder_slots` row. Also mirrors the placed card into that
   *  row's card_id/card_name/image_url so BinderViewPage's direct read of
   *  binder_slots — the older, simpler binder feature — shows the same
   *  thing this system does, instead of two disagreeing views of one binder. */
  def placeInBinderSlot(userId: String, binderId: String, slotIndex: Int, lotId: String,
    quantity: Int, protection: Option[String], notes: Option[String]): Task[CardAllocation]
  def removeAllocation(userId: String, allocationId: String): Task[Unit]

object RoomRepository:
  final class Live(xa: Transactor[Task]) extends RoomRepository:
    private type SpaceRow = (String,String,String,String,Boolean,Int,Instant,Instant)
    private type UnitRow = (String,String,String,String,String,String,Int,Int,Int,Int,String,Instant,Instant)
    private type CaseRow = (String,String,String,String,String,String,String,Boolean,Int,Int,Int,String,Instant,Instant)

    def listSpaces(userId: String): Task[List[CollectionSpace]] =
      // Spaces is the logged-in landing page, so a user must never see an
      // empty "create a space" prompt there — ensure a default space exists
      // before reading. Guarded by NOT EXISTS (skip once the user has any
      // space) and ON CONFLICT against uq_collection_spaces_default (the
      // partial unique index on is_default) so two concurrent first loads
      // can't both insert a default space.
      val defaultId = java.util.UUID.randomUUID().toString
      val now = Instant.now()
      val ensureDefault = sql"""INSERT INTO collection_spaces(id,user_id,name,space_type,is_default,position,created_at,updated_at)
        SELECT $defaultId,$userId,'My Collection Room','collection_room',TRUE,0,$now,$now
        WHERE NOT EXISTS (SELECT 1 FROM collection_spaces WHERE user_id=$userId)
        ON CONFLICT (user_id) WHERE is_default DO NOTHING""".update.run
      val spaces = sql"""SELECT id,user_id,name,space_type,is_default,position,created_at,updated_at
        FROM collection_spaces WHERE user_id=$userId ORDER BY position,created_at""".query[SpaceRow].to[List]
      val units = sql"""SELECT su.id,su.space_id,su.name,su.unit_type,su.preset,su.color,su.shelf_count,
        su.positions_per_shelf,su.max_stack_height,su.position,su.config::text,su.created_at,su.updated_at
        FROM storage_units su JOIN collection_spaces s ON s.id=su.space_id
        WHERE s.user_id=$userId ORDER BY su.space_id,su.position,su.created_at""".query[UnitRow].to[List]
      val cases = sql"""SELECT dc.id,dc.space_id,dc.name,dc.case_type,dc.preset,dc.frame_color,dc.light_color,
        dc.light_enabled,dc.shelf_count,dc.slots_per_shelf,dc.position,dc.config::text,dc.created_at,dc.updated_at
        FROM display_cases dc JOIN collection_spaces s ON s.id=dc.space_id
        WHERE s.user_id=$userId ORDER BY dc.space_id,dc.position,dc.created_at""".query[CaseRow].to[List]
      val slots = sql"""SELECT ds.id,ds.display_case_id,ds.shelf_index,ds.slot_index,ds.label
        FROM display_slots ds JOIN display_cases dc ON dc.id=ds.display_case_id
        JOIN collection_spaces s ON s.id=dc.space_id WHERE s.user_id=$userId
        ORDER BY ds.display_case_id,ds.shelf_index,ds.slot_index"""
        .query[(String,String,Int,Int,Option[String])].to[List]
      (for
        _ <- ensureDefault
        sr <- spaces
        ur <- units
        cr <- cases
        sl <- slots
      yield
        val unitsBySpace = ur.map(StorageUnit.apply.tupled).groupBy(_.spaceId)
        val slotsByCase = sl.map(DisplaySlot.apply.tupled).groupBy(_.displayCaseId)
        val casesBySpace = cr.map { case (id,sid,n,t,p,fc,lc,le,sc,sps,pos,cfg,ca,ua) =>
          DisplayCase(id,sid,n,t,p,fc,lc,le,sc,sps,pos,cfg,slotsByCase.getOrElse(id,Nil),ca,ua)
        }.groupBy(_.spaceId)
        sr.map { case (id,uid,n,t,d,pos,ca,ua) =>
          CollectionSpace(id,uid,n,t,d,pos,unitsBySpace.getOrElse(id,Nil),casesBySpace.getOrElse(id,Nil),ca,ua)
        }
      ).transact(xa)

    def createSpace(userId: String, name: String, spaceType: String): Task[CollectionSpace] =
      val id = java.util.UUID.randomUUID().toString
      val now = Instant.now()
      (for
        pos <- sql"SELECT COALESCE(MAX(position)+1,0) FROM collection_spaces WHERE user_id=$userId".query[Int].unique
        _ <- sql"""INSERT INTO collection_spaces(id,user_id,name,space_type,is_default,position,created_at,updated_at)
          VALUES($id,$userId,$name,$spaceType,FALSE,$pos,$now,$now)""".update.run
      yield CollectionSpace(id,userId,name,spaceType,false,pos,Nil,Nil,now,now)).transact(xa)

    def createStorageUnit(userId: String, spaceId: String, name: String, unitType: String,
      preset: String, color: String, shelfCount: Int, positionsPerShelf: Int,
      maxStackHeight: Int, config: String): Task[StorageUnit] =
      val id = java.util.UUID.randomUUID().toString
      val now = Instant.now()
      (for
        owned <- sql"SELECT COUNT(*) FROM collection_spaces WHERE id=$spaceId AND user_id=$userId".query[Int].unique
        _ <- if owned == 1 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("Space not found"))
        pos <- sql"SELECT COALESCE(MAX(position)+1,0) FROM storage_units WHERE space_id=$spaceId".query[Int].unique
        _ <- sql"""INSERT INTO storage_units(id,space_id,name,unit_type,preset,color,shelf_count,
          positions_per_shelf,max_stack_height,position,config,created_at,updated_at)
          VALUES($id,$spaceId,$name,$unitType,$preset,$color,$shelfCount,$positionsPerShelf,$maxStackHeight,$pos,$config::jsonb,$now,$now)""".update.run
      yield StorageUnit(id,spaceId,name,unitType,preset,color,shelfCount,positionsPerShelf,maxStackHeight,pos,config,now,now)).transact(xa)

    def placeBox(userId: String, boxId: String, spaceId: String, unitId: String,
      shelfIndex: Int, stackIndex: Int, stackLevel: Int): Task[Unit] =
      (for
        valid <- sql"""SELECT COUNT(*) FROM storage_units su JOIN collection_spaces s ON s.id=su.space_id
          WHERE su.id=$unitId AND su.space_id=$spaceId AND s.user_id=$userId
            AND $shelfIndex < su.shelf_count AND $stackIndex < su.positions_per_shelf
            AND $stackLevel < su.max_stack_height""".query[Int].unique
        _ <- if valid == 1 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("Invalid box placement"))
        // Stack level alone isn't enough — a box already resting at this
        // shelf/stack but a different level would silently block the real
        // conflict this is meant to catch (two boxes on the same level).
        collision <- sql"""SELECT COUNT(*) FROM storage_boxes
          WHERE storage_unit_id=$unitId AND shelf_index=$shelfIndex
            AND stack_index=$stackIndex AND stack_level=$stackLevel AND id<>$boxId""".query[Int].unique
        _ <- if collision == 0 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("That stack position is already occupied"))
        n <- sql"""UPDATE storage_boxes SET space_id=$spaceId,storage_unit_id=$unitId,shelf_index=$shelfIndex,
          stack_index=$stackIndex,stack_level=$stackLevel,updated_at=NOW() WHERE id=$boxId AND user_id=$userId""".update.run
        _ <- if n == 1 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("Box not found"))
      yield ()).transact(xa)

    def placeBinder(userId: String, binderId: String, spaceId: String, unitId: String,
      shelfIndex: Int, shelfPosition: Int): Task[Unit] =
      (for
        valid <- sql"""SELECT COUNT(*) FROM storage_units su JOIN collection_spaces s ON s.id=su.space_id
          WHERE su.id=$unitId AND su.space_id=$spaceId AND s.user_id=$userId
            AND $shelfIndex < su.shelf_count AND $shelfPosition < su.positions_per_shelf""".query[Int].unique
        _ <- if valid == 1 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("Invalid binder placement"))
        collision <- sql"""SELECT COUNT(*) FROM binders
          WHERE storage_unit_id=$unitId AND shelf_index=$shelfIndex AND shelf_position=$shelfPosition
            AND id<>$binderId""".query[Int].unique
        _ <- if collision == 0 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("That shelf position is already occupied"))
        n <- sql"""UPDATE binders SET space_id=$spaceId,storage_unit_id=$unitId,shelf_index=$shelfIndex,
          shelf_position=$shelfPosition,updated_at=NOW() WHERE id=$binderId AND user_id=$userId""".update.run
        _ <- if n == 1 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("Binder not found"))
      yield ()).transact(xa)

    def createDisplayCase(userId: String, spaceId: String, name: String, caseType: String,
      preset: String, frameColor: String, lightColor: String, shelfCount: Int,
      slotsPerShelf: Int, config: String): Task[DisplayCase] =
      val id = java.util.UUID.randomUUID().toString
      val now = Instant.now()
      (for
        owned <- sql"SELECT COUNT(*) FROM collection_spaces WHERE id=$spaceId AND user_id=$userId".query[Int].unique
        _ <- if owned == 1 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("Space not found"))
        pos <- sql"SELECT COALESCE(MAX(position)+1,0) FROM display_cases WHERE space_id=$spaceId".query[Int].unique
        _ <- sql"""INSERT INTO display_cases(id,space_id,name,case_type,preset,frame_color,light_color,
          shelf_count,slots_per_shelf,position,config,created_at,updated_at)
          VALUES($id,$spaceId,$name,$caseType,$preset,$frameColor,$lightColor,$shelfCount,$slotsPerShelf,$pos,$config::jsonb,$now,$now)""".update.run
        slotRows = (0 until shelfCount).flatMap(s => (0 until slotsPerShelf).map(i =>
          DisplaySlot(java.util.UUID.randomUUID().toString,id,s,i,None))).toList
        _ <- slotRows.traverse_(slot => sql"""INSERT INTO display_slots(id,display_case_id,shelf_index,slot_index)
          VALUES(${slot.id},$id,${slot.shelfIndex},${slot.slotIndex})""".update.run)
      yield DisplayCase(id,spaceId,name,caseType,preset,frameColor,lightColor,true,shelfCount,slotsPerShelf,pos,config,slotRows,now,now)).transact(xa)

    def setDisplayLights(userId: String, caseId: String, enabled: Boolean): Task[Unit] =
      sql"""UPDATE display_cases dc SET light_enabled=$enabled,updated_at=NOW()
        WHERE dc.id=$caseId AND EXISTS(SELECT 1 FROM collection_spaces s WHERE s.id=dc.space_id AND s.user_id=$userId)"""
        .update.run.void.transact(xa)

    def listLots(userId: String): Task[List[InventoryLot]] =
      sql"""SELECT l.id,l.user_id,l.card_id,l.variant_key,l.edition,l.language,l.condition,l.quantity,
        COALESCE(SUM(a.quantity),0),l.created_at,l.updated_at
        FROM inventory_lots l LEFT JOIN card_allocations a ON a.lot_id=l.id
        WHERE l.user_id=$userId GROUP BY l.id ORDER BY l.updated_at DESC"""
        .query[(String,String,String,String,String,String,String,Int,Int,Instant,Instant)]
        .to[List].map(_.map(InventoryLot.apply.tupled)).transact(xa)

    def listDrawerAllocations(userId: String, drawerId: String): Task[List[CardAllocation]] =
      sql"""SELECT a.id,a.lot_id,a.drawer_id,a.binder_slot_id,a.display_slot_id,a.quantity,
        a.protection,a.notes,a.created_at,a.updated_at FROM card_allocations a
        JOIN inventory_lots l ON l.id=a.lot_id
        JOIN storage_drawers d ON d.id=a.drawer_id
        JOIN storage_boxes b ON b.id=d.box_id
        WHERE l.user_id=$userId AND b.user_id=$userId AND a.drawer_id=$drawerId
        ORDER BY a.created_at DESC"""
        .query[(String,String,Option[String],Option[String],Option[String],Int,Option[String],Option[String],Instant,Instant)]
        .to[List].map(_.map(CardAllocation.apply.tupled)).transact(xa)

    def listCaseAllocations(userId: String, caseId: String): Task[List[CardAllocation]] =
      sql"""SELECT a.id,a.lot_id,a.drawer_id,a.binder_slot_id,a.display_slot_id,a.quantity,
        a.protection,a.notes,a.created_at,a.updated_at FROM card_allocations a
        JOIN inventory_lots l ON l.id=a.lot_id
        JOIN display_slots ds ON ds.id=a.display_slot_id
        JOIN display_cases dc ON dc.id=ds.display_case_id
        JOIN collection_spaces s ON s.id=dc.space_id
        WHERE l.user_id=$userId AND s.user_id=$userId AND dc.id=$caseId
        ORDER BY a.created_at DESC"""
        .query[(String,String,Option[String],Option[String],Option[String],Int,Option[String],Option[String],Instant,Instant)]
        .to[List].map(_.map(CardAllocation.apply.tupled)).transact(xa)

    def listBinderAllocations(userId: String, binderId: String): Task[List[CardAllocation]] =
      sql"""SELECT a.id,a.lot_id,a.drawer_id,a.binder_slot_id,a.display_slot_id,a.quantity,
        a.protection,a.notes,a.created_at,a.updated_at FROM card_allocations a
        JOIN inventory_lots l ON l.id=a.lot_id
        JOIN binder_slots bs ON bs.id=a.binder_slot_id
        JOIN binders b ON b.id=bs.binder_id
        WHERE l.user_id=$userId AND b.user_id=$userId AND b.id=$binderId
        ORDER BY a.created_at DESC"""
        .query[(String,String,Option[String],Option[String],Option[String],Int,Option[String],Option[String],Instant,Instant)]
        .to[List].map(_.map(CardAllocation.apply.tupled)).transact(xa)

    def allocate(userId: String, lotId: String, drawerId: Option[String],
      binderSlotId: Option[String], displaySlotId: Option[String], quantity: Int,
      protection: Option[String], notes: Option[String]): Task[CardAllocation] =
      val id = java.util.UUID.randomUUID().toString
      val now = Instant.now()
      (for
        _ <- if quantity > 0 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("Quantity must be positive"))
        // FOR UPDATE locks the lot row for the rest of this transaction, so two
        // concurrent allocations of the same lot can't both read the same
        // "available" count and both pass the check below.
        owned <- sql"SELECT quantity FROM inventory_lots WHERE id=$lotId AND user_id=$userId FOR UPDATE".query[Int].option
        total <- owned match
          case None => FC.raiseError[Int](RuntimeException("Inventory lot not found"))
          case Some(q) => q.pure[ConnectionIO]
        allocated <- sql"SELECT COALESCE(SUM(quantity),0) FROM card_allocations WHERE lot_id=$lotId".query[Int].unique
        _ <- if allocated + quantity <= total then ().pure[ConnectionIO]
             else FC.raiseError(RuntimeException(s"Only ${total - allocated} of this card are available to place"))
        _ <- drawerId match
          case None => ().pure[ConnectionIO]
          case Some(d) =>
            sql"""SELECT COUNT(*) FROM storage_drawers dr JOIN storage_boxes b ON b.id=dr.box_id
              WHERE dr.id=$d AND b.user_id=$userId""".query[Int].unique.flatMap(n =>
              if n == 1 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("Box drawer not found")))
        _ <- binderSlotId match
          case None => ().pure[ConnectionIO]
          case Some(bs) =>
            for
              belongs <- sql"""SELECT COUNT(*) FROM binder_slots s
                JOIN binders b ON b.id=s.binder_id
                WHERE s.id=$bs AND b.user_id=$userId""".query[Int].unique
              _ <- if belongs == 1 then ().pure[ConnectionIO]
                   else FC.raiseError(RuntimeException("Binder slot not found"))
              occupied <- sql"SELECT COUNT(*) FROM card_allocations WHERE binder_slot_id=$bs".query[Int].unique
              _ <- if occupied == 0 then ().pure[ConnectionIO]
                   else FC.raiseError(RuntimeException("That binder slot is already occupied"))
            yield ()
        _ <- displaySlotId match
          case None => ().pure[ConnectionIO]
          case Some(ds) =>
            for
              belongs <- sql"""SELECT COUNT(*) FROM display_slots s
                JOIN display_cases dc ON dc.id=s.display_case_id
                JOIN collection_spaces sp ON sp.id=dc.space_id
                WHERE s.id=$ds AND sp.user_id=$userId""".query[Int].unique
              _ <- if belongs == 1 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("Display slot not found"))
              occupied <- sql"SELECT COUNT(*) FROM card_allocations WHERE display_slot_id=$ds".query[Int].unique
              _ <- if occupied == 0 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("That display slot is already occupied"))
            yield ()
        _ <- sql"""INSERT INTO card_allocations(id,lot_id,drawer_id,binder_slot_id,display_slot_id,
          quantity,protection,notes,created_at,updated_at)
          VALUES($id,$lotId,$drawerId,$binderSlotId,$displaySlotId,$quantity,$protection,$notes,$now,$now)""".update.run
      yield CardAllocation(id,lotId,drawerId,binderSlotId,displaySlotId,quantity,protection,notes,now,now)).transact(xa)

    def placeInBinderSlot(userId: String, binderId: String, slotIndex: Int, lotId: String,
      quantity: Int, protection: Option[String], notes: Option[String]): Task[CardAllocation] =
      val id = java.util.UUID.randomUUID().toString
      val slotId = java.util.UUID.randomUUID().toString
      val now = Instant.now()
      (for
        _ <- if quantity == 1 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("A binder slot holds exactly one copy"))
        ownsBinder <- sql"SELECT COUNT(*) FROM binders WHERE id=$binderId AND user_id=$userId".query[Int].unique
        _ <- if ownsBinder == 1 then ().pure[ConnectionIO] else FC.raiseError(RuntimeException("Binder not found"))
        owned <- sql"SELECT quantity FROM inventory_lots WHERE id=$lotId AND user_id=$userId FOR UPDATE".query[Int].option
        total <- owned match
          case None => FC.raiseError[Int](RuntimeException("Inventory lot not found"))
          case Some(q) => q.pure[ConnectionIO]
        allocated <- sql"SELECT COALESCE(SUM(quantity),0) FROM card_allocations WHERE lot_id=$lotId".query[Int].unique
        _ <- if allocated + quantity <= total then ().pure[ConnectionIO]
             else FC.raiseError(RuntimeException(s"Only ${total - allocated} of this card are available to place"))
        // Slots are lazily created — INSERT the row the first time anything
        // (old direct-cardId system or this one) touches this position.
        _ <- sql"""INSERT INTO binder_slots(id,binder_id,slot_index) VALUES ($slotId,$binderId,$slotIndex)
          ON CONFLICT (binder_id,slot_index) DO NOTHING""".update.run
        resolved <- sql"SELECT id,card_id FROM binder_slots WHERE binder_id=$binderId AND slot_index=$slotIndex"
          .query[(String,Option[String])].unique
        occupiedByAllocation <- sql"SELECT COUNT(*) FROM card_allocations WHERE binder_slot_id=${resolved._1}".query[Int].unique
        _ <- if resolved._2.isEmpty && occupiedByAllocation == 0 then ().pure[ConnectionIO]
             else FC.raiseError(RuntimeException("That binder slot is already occupied"))
        card <- sql"""SELECT c.name,c.image_small,c.id FROM cards c
          JOIN inventory_lots l ON l.card_id=c.id WHERE l.id=$lotId""".query[(String,String,String)].option
        _ <- sql"""INSERT INTO card_allocations(id,lot_id,drawer_id,binder_slot_id,display_slot_id,
          quantity,protection,notes,created_at,updated_at)
          VALUES($id,$lotId,NULL,${resolved._1},NULL,$quantity,$protection,$notes,$now,$now)""".update.run
        _ <- card match
          case None => ().pure[ConnectionIO]
          case Some((cardName, imageUrl, cardId)) =>
            sql"""UPDATE binder_slots SET card_id=$cardId,card_name=$cardName,image_url=$imageUrl
              WHERE id=${resolved._1}""".update.run.void
        _ <- sql"UPDATE binders SET updated_at=NOW() WHERE id=$binderId".update.run.void
      yield CardAllocation(id,lotId,None,Some(resolved._1),None,quantity,protection,notes,now,now)).transact(xa)

    def removeAllocation(userId: String, allocationId: String): Task[Unit] =
      (for
        slot <- sql"""SELECT a.binder_slot_id FROM card_allocations a JOIN inventory_lots l ON l.id=a.lot_id
          WHERE a.id=$allocationId AND l.user_id=$userId""".query[Option[String]].option
        _ <- sql"""DELETE FROM card_allocations a USING inventory_lots l
          WHERE a.id=$allocationId AND a.lot_id=l.id AND l.user_id=$userId""".update.run
        // Keep BinderViewPage's direct read of binder_slots in sync — it
        // doesn't know about card_allocations at all, so a removal here has
        // to clear the cached card_id/card_name/image_url itself.
        _ <- slot.flatten match
          case None => ().pure[ConnectionIO]
          case Some(slotId) =>
            sql"UPDATE binder_slots SET card_id=NULL,card_name=NULL,image_url=NULL WHERE id=$slotId".update.run.void
      yield ()).transact(xa)

  val layer: ZLayer[Transactor[Task], Nothing, RoomRepository] = ZLayer.fromFunction(new Live(_))
