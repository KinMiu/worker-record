package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2/log"

	"github.com/KinMiu/worker-record/config"
	"github.com/KinMiu/worker-record/pkg/dto"
	"github.com/KinMiu/worker-record/pkg/model"
)

// DeviceClientService interacts with the central management backend REST API.
type DeviceClientService struct {
	httpClient *http.Client
}

// NewDeviceClientService initializes a DeviceClientService instance.
func NewDeviceClientService() *DeviceClientService {
	return &DeviceClientService{
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// FetchDevices retrieves the list of camera devices assigned to this worker.
func (c *DeviceClientService) FetchDevices() ([]model.Device, error) {
	apiBaseURL := config.API_BASE_URL.GetValue()
	workerID := config.WORKER_ID.GetValue()
	apiKeyHeader := config.API_KEY_HEADER.GetValueOrDefault("x-api-key")
	apiKey := config.API_KEY.GetValue()
	apiAuthToken := config.API_AUTH_TOKEN.GetValue()

	if apiBaseURL == "" {
		return nil, fmt.Errorf("API_BASE_URL is not configured")
	}

	req, err := http.NewRequest(http.MethodGet, apiBaseURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to construct HTTP request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	if workerID != "" {
		req.Header.Set("X-Worker-ID", workerID)
	}

	if apiKey != "" {
		req.Header.Set(apiKeyHeader, apiKey)
	}

	if apiAuthToken != "" {
		authToken := apiAuthToken
		if !strings.HasPrefix(strings.ToLower(authToken), "bearer ") {
			authToken = "Bearer " + authToken
		}
		req.Header.Set("Authorization", authToken)
	}

	log.Infof("[API] Querying device list from %s (Worker: %s)...", apiBaseURL, workerID)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned non-200 HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var apiResp dto.APIResponseDTO
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("failed to decode JSON response: %w", err)
	}

	log.Infof("[API] Successfully retrieved %d device(s) from REST API", len(apiResp.Data))
	return apiResp.Data, nil
}
