package dto

import "github.com/KinMiu/worker-record/pkg/model"

// APIResponseDTO represents the standard response payload envelope from the central REST API.
type APIResponseDTO struct {
	Code    int            `json:"code,omitempty"`
	Status  any            `json:"status,omitempty"`
	Message string         `json:"message,omitempty"`
	Data    []model.Device `json:"data"`
}
