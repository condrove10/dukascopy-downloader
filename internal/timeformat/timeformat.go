package timeformat

import (
	"time"
)

func GetDateTimeRange(start, end time.Time, hourInterval int) []time.Time {
	now := time.Now()

	currentHour := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location())
	if currentHour.Equal(time.Date(end.Year(), end.Month(), end.Day(), end.Hour(), 0, 0, 0, end.Location())) {
		end = time.Date(end.Year(), end.Month(), end.Day(), end.Hour()-1, 0, 0, 0, end.Location())
	}

	if end.After(now) {
		end = time.Date(currentHour.Year(), currentHour.Month(), currentHour.Day(), currentHour.Hour(), 0, 0, 0, currentHour.Location())
	}

	var times []time.Time
	for t := time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), 0, 0, 0, start.Location()); t.Before(end); t =
		t.Add(time.Duration(hourInterval) * time.Hour) {
		times = append(times, t)
	}
	return times
}
