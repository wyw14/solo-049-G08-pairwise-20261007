package api

import (
	"net/http"
	"strconv"
	"time"

	"orbitlink/internal/storm"
)

func (r *Runtime) assessStorm(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		StationID           string    `json:"station_id"`
		PassID              string    `json:"pass_id"`
		AntennaID           string    `json:"antenna_id"`
		ChainID             string    `json:"chain_id"`
		MetersPerSecond     float64   `json:"meters_per_second"`
		GustMetersPerSecond float64   `json:"gust_meters_per_second"`
		ObservedAt          time.Time `json:"observed_at"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	if input.StationID == "" {
		input.StationID = "orbitlink"
	}
	sample := storm.Sample{StationID: input.StationID, MetersPerSecond: input.MetersPerSecond, GustMetersPerSecond: input.GustMetersPerSecond, ObservedAt: input.ObservedAt}
	record, err := r.Storm.Handle(request.Context(), sample, input.PassID, input.AntennaID, input.ChainID)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, record)
}

func (r *Runtime) listStormRecords(writer http.ResponseWriter, request *http.Request) {
	limit := 0
	if raw := request.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			writeError(writer, strconv.ErrSyntax)
			return
		}
		limit = value
	}
	records, err := r.Storm.Records(limit)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": records})
}
