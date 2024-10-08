package timeformat

import "time"

func GetDateTimeRange(start, end time.Time, hourInterval int) []time.Time {
	var times []time.Time
	for t := start; t.Before(end); t = t.Add(time.Duration(hourInterval) * time.Hour) {
		times = append(times, t)
	}
	return times
}
