package utils

import (
	"github.com/gofiber/fiber/v2"

	"github.com/KinMiu/worker-record/pkg/model"
)

// SuccessResponse formats and returns a standard ResponseEntity JSON response.
func SuccessResponse[T any](ctx *fiber.Ctx, code int, message string, data T, meta *model.MetaPagination) error {
	response := model.ResponseEntity[T]{
		Code:    code,
		Status:  true,
		Message: message,
		Data:    data,
		Meta:    meta,
	}
	return ctx.Status(code).JSON(response)
}

// ErrorResponse formats and returns a standard ResponseError JSON response.
func ErrorResponse[T any](ctx *fiber.Ctx, code int, message string, data T) error {
	response := model.ResponseError[T]{
		ResponseEntity: model.ResponseEntity[T]{
			Code:    code,
			Status:  false,
			Message: message,
			Data:    data,
		},
		Path: ctx.Path(),
	}
	return ctx.Status(code).JSON(response)
}
