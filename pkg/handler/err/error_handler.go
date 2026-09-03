package err

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	"github.com/KinMiu/worker-record/pkg/model"
)

// ErrorHandler provides centralized error catching for Fiber applications.
func ErrorHandler(c *fiber.Ctx, err error) error {
	code := fiber.StatusInternalServerError
	message := "Internal Server Error"

	var e *fiber.Error
	if errors.As(err, &e) {
		code = e.Code
		message = e.Message
	}

	response := model.ResponseError[any]{
		ResponseEntity: model.ResponseEntity[any]{
			Code:    code,
			Status:  false,
			Message: message,
			Data:    nil,
		},
		Path: c.Path(),
	}

	return c.Status(code).JSON(response)
}
