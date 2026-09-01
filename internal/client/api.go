package client

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/KinMiu/worker-record/internal/config"
	"github.com/KinMiu/worker-record/internal/models"
)

// APIClient interacts with the central management backend REST API.
type APIClient struct {
	cfg        *config.Config
	httpClient *http.Client
}

// NewAPIClient initializes an APIClient instance with reasonable HTTP timeouts.
func NewAPIClient(cfg *config.Config) *APIClient {
	return &APIClient{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// FetchDevices retrieves the list of camera devices assigned to this worker.
func (c *APIClient) FetchDevices() ([]models.Device, error) {
	req, err := http.NewRequest(http.MethodGet, c.cfg.APIBaseURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to construct HTTP request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Worker-ID", c.cfg.WorkerID)

	if c.cfg.APIKey != "" {
		headerName := c.cfg.APIKeyHeader
		if headerName == "" {
			headerName = "x-api-key"
		}
		req.Header.Set(headerName, c.cfg.APIKey)
	}

	if c.cfg.APIAuthToken != "" {
		authToken := c.cfg.APIAuthToken
		if !strings.HasPrefix(strings.ToLower(authToken), "bearer ") {
			authToken = "Bearer " + authToken
		}
		req.Header.Set("Authorization", authToken)
	}

	log.Printf("[API] Querying device list from %s (Worker: %s)...", c.cfg.APIBaseURL, c.cfg.WorkerID)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned non-200 HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var apiResp models.APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("failed to decode JSON response: %w", err)
	}

	log.Printf("[API] Successfully retrieved %d device(s) from REST API", len(apiResp.Data))
	return apiResp.Data, nil
}
