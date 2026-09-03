package router

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/swagger"

	_ "github.com/KinMiu/worker-record/docs"
	"github.com/KinMiu/worker-record/pkg/handler/http"
)

// SetupRoutes registers standard middlewares, Swagger docs, and HTTP route handlers.
func SetupRoutes(app *fiber.App, healthHandler *http.HealthHandler) {
	// Standard Fiber Middlewares (LSKK Section 1)
	app.Use(recover.New())
	app.Use(cors.New())
	app.Use(compress.New())
	app.Use(logger.New())

	// Swagger Documentation
	app.Get("/swagger/*", swagger.HandlerDefault)

	// Application API Routes
	api := app.Group("/api/v1")
	api.Get("/health", healthHandler.CheckHealth)

	// Root health alias
	app.Get("/health", healthHandler.CheckHealth)
}
