package api

import (
	"net/http"
	"time"

	"orbitlink/internal/storm"
)

func (r *Runtime) stormDisposition(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		StationID           string    `json:"station_id"`
		MetersPerSecond     float64   `json:"meters_per_second"`
		GustMetersPerSecond float64   `json:"gust_meters_per_second"`
		DirectionDegrees    float64   `json:"direction_degrees"`
		ObservedAt          time.Time `json:"observed_at"`
		PassID              string    `json:"pass_id"`
		AntennaID           string    `json:"antenna_id"`
		ChainID             string    `json:"chain_id"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	if input.ObservedAt.IsZero() {
		input.ObservedAt = time.Now().UTC()
	}
	if input.StationID == "" {
		input.StationID = "orbitlink"
	}
	disposition, err := r.Storm.Handle(request.Context(), storm.Sample{
		StationID: input.StationID, MetersPerSecond: input.MetersPerSecond,
		GustMetersPerSecond: input.GustMetersPerSecond, DirectionDegrees: input.DirectionDegrees,
		ObservedAt: input.ObservedAt,
	}, storm.Request{PassID: input.PassID, AntennaID: input.AntennaID, ChainID: input.ChainID})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, disposition)
}

func (r *Runtime) listStormOperations(writer http.ResponseWriter, _ *http.Request) {
	operations, err := r.Storm.Operations()
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": operations})
}
