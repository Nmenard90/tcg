/**
 * Main — application entry point. Reads DB config, opens the connection,
 * wires every repository/service/route layer together, and starts the
 * HTTP server.
 *
 * HOW IT WORKS
 *   Layers build strictly bottom-up: repositories (need only the DB
 *   connection) -> services (need repositories, and in RipTrackerService's
 *   case, other already-built services) -> routes (need services). See
 *   inline comments below for why RipTrackerService and PriceService are
 *   each built as their own step rather than folded into the main batch.
 *
 * DEPENDS ON: All routes, services, repositories, DatabaseConfig
 */

package com.poketracker

import com.poketracker.api.*
import com.poketracker.config.DatabaseConfig
import com.poketracker.repository.*
import com.poketracker.security.{AuthGuard, SupabaseJwtVerifier}
import com.poketracker.service.*
import zio.*
import zio.http.*

object Main extends ZIOAppDefault:

  val run: ZIO[Any, Throwable, Unit] =
    // Scope guarantees the DB connection closes if the app ever stops.
    ZIO.scoped {
      for
        config     <- DatabaseConfig.fromEnv
        transactor <- DatabaseConfig.makeTransactor(config)
        _          <- ZIO.logInfo("Database connection pool ready")
        port       <- System.env("PORT").map(_.flatMap(_.toIntOption).getOrElse(8080))

        repoLayer   = ZLayer.succeed(transactor) >>>
                      (CardRepository.layer ++
                       CollectionRepository.layer ++
                       UserRepository.layer ++
                       BinderRepository.layer ++
                       RipTrackerRepository.layer ++
                       StorageRepository.layer ++
                       RoomRepository.layer ++
                       ShelfRepository.layer)

        // Built separately so CardService.layer can depend on it directly below.
        priceLayer  = repoLayer >>> PriceService.layer

        coreServices = (repoLayer ++ priceLayer) >>>
                      (CardService.layer ++
                       CollectionService.layer ++
                       UserService.layer ++
                       BinderService.layer ++
                       StorageService.layer ++
                       RoomService.layer)

        // Needs both the raw repositories AND the already-built core
        // services above (not just their repositories), so it can't join the batch.
        ripLayer    = (repoLayer ++ coreServices) >>> RipTrackerService.layer

        // Same reason as ripLayer: ShelfService reads through the already-built
        // StorageService/BinderService rather than their raw repositories.
        shelfLayer  = (repoLayer ++ coreServices) >>> ShelfService.layer

        appLayer    = coreServices ++ ripLayer ++ shelfLayer ++ SupabaseJwtVerifier.layer

        // Public: no Supabase session required (catalog browsing, health check,
        // and the read-only rip-or-hold calculation). Catalog refresh/admin
        // mutations intentionally have no mounted HTTP routes until a trusted
        // admin authorization mechanism exists.
        publicRoutes    = Routes(Method.GET / "health" -> Handler.ok) ++
                          CardRoutes.publicRoutes ++
                          RipRoutes.verdictRoutes

        // Protected: AuthGuard verifies the caller's Supabase token, provisions/
        // loads their profile, and 403s any request whose path userId doesn't
        // match — see security/AuthGuard.scala for exactly what's covered.
        protectedRoutes = (CollectionRoutes.routes ++
                           UserRoutes.routes ++
                           BinderRoutes.routes ++
                           StorageRoutes.routes ++
                           ShelfRoutes.routes ++
                           RoomRoutes.routes ++
                           RipRoutes.sessionRoutes ++
                           AuthRoutes.routes) @@ AuthGuard.aspect

        allRoutes       = publicRoutes ++ protectedRoutes

        // Required so the frontend (served from a different origin) isn't
        // blocked by browser CORS checks.
        cors        = Middleware.cors(Middleware.CorsConfig(
                        allowedOrigin    = _ => Some(Header.AccessControlAllowOrigin.All),
                        allowedMethods   = Header.AccessControlAllowMethods.All,
                        allowedHeaders   = Header.AccessControlAllowHeaders.All,
                        exposedHeaders   = Header.AccessControlExposeHeaders.None,
                        maxAge           = None
                      ))

        // Demo ("try it, no signup") accounts are Supabase anonymous-auth
        // sessions — see security/AuthGuard.scala and UserService. Nothing
        // deletes them on its own, so this sweeps rows past their 24h
        // window once an hour for as long as the server runs. Forked
        // before Server.serve so it shares the one built appLayer instead
        // of re-running layer construction (e.g. the JWKS fetch) a second time.
        cleanupJob  = ZIO.serviceWithZIO[UserService](_.cleanupExpiredAnonymousUsers)
                        .tap(n => ZIO.when(n > 0)(ZIO.logInfo(s"Cleaned up $n expired demo account(s)")))
                        .catchAll(e => ZIO.logWarning(s"Demo account cleanup failed: ${e.getMessage}"))
                        .repeat(Schedule.spaced(1.hour))

        serverEffect = cleanupJob.forkDaemon *> Server.serve(allRoutes @@ cors)

        _          <- ZIO.logInfo(s"Starting PokéTracker API on port $port")
        _          <- serverEffect.provide(appLayer, Server.defaultWithPort(port))
      yield ()
    }
