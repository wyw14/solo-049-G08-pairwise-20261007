package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStormDispositionEndpointDedupAndOperations(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()

	value := runtime.Passes.List()[0]
	observedAt := time.Now().UTC()
	payload := func() string {
		data, _ := json.Marshal(map[string]any{
			"pass_id": value.ID, "antenna_id": value.AntennaID, "chain_id": "RF-01",
			"meters_per_second": 15, "gust_meters_per_second": 25, "observed_at": observedAt,
		})
		return string(data)
	}

	first := requestJSON(t, server.Client(), http.MethodPost, server.URL+"/api/storm/disposition", payload())
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first disposition returned %d", first.StatusCode)
	}
	var decision map[string]any
	decodeBody(t, first, &decision)
	if decision["decision"] != "stow_and_suspend" {
		t.Fatalf("decision = %v, want stow_and_suspend", decision["decision"])
	}
	operationID := decision["operation_id"]

	// The identical weather sample must never cause a second stow.
	second := requestJSON(t, server.Client(), http.MethodPost, server.URL+"/api/storm/disposition", payload())
	if second.StatusCode != http.StatusOK {
		t.Fatalf("duplicate disposition returned %d", second.StatusCode)
	}
	var repeated map[string]any
	decodeBody(t, second, &repeated)
	if repeated["decision"] != "duplicate" || repeated["operation_id"] != operationID {
		t.Fatalf("duplicate sample mishandled: %#v", repeated)
	}

	operationsResponse, err := server.Client().Get(server.URL + "/api/storm/operations")
	if err != nil {
		t.Fatal(err)
	}
	defer operationsResponse.Body.Close()
	var operations struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.NewDecoder(operationsResponse.Body).Decode(&operations); err != nil {
		t.Fatal(err)
	}
	if len(operations.Items) != 1 || operations.Items[0]["id"] != operationID {
		t.Fatalf("immutable storm journal mismatch: %#v", operations.Items)
	}

	// Planned pass's commands were retired, ready for re-issue at recovery.
	for _, command := range runtime.Commands.List(value.ID) {
		if !command.Retired {
			t.Fatal("live pointing command survived storm stow")
		}
	}
}
