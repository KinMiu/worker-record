package model

// MetaPagination represents the standard pagination metadata envelope.
type MetaPagination struct {
	Page      int `json:"page"`
	Limit     int `json:"limit"`
	TotalPage int `json:"totalPage"`
	TotalData int `json:"totalData"`
}

// ResponseEntity defines the standard response envelope for PT. LSKK services.
type ResponseEntity[T any] struct {
	Code    int             `json:"code"`
	Status  bool            `json:"status"`
	Message string          `json:"message"`
	Data    T               `json:"data"`
	Meta    *MetaPagination `json:"meta,omitempty"`
}

// ResponseError defines the standard error response envelope for PT. LSKK services.
type ResponseError[T any] struct {
	ResponseEntity[T]
	Path string `json:"path"`
}
